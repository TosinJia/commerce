package com.itstar.session

import java.util.{Date, UUID}

import com.itstar.commons.conf.ConfigurationManager
import com.itstar.commons.constant.Constants
import com.itstar.commons.model.{UserInfo, UserVisitAction}
import com.itstar.commons.utils.{DateUtils, ParamUtils, StringUtils}
import net.sf.json.JSONObject
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object UserVisitSessionAnalyze1 {
  /**
    * 对session数据聚合
    * @param spark
    * @param sessionId2actionRDD sessionId为key，UserVisitAction为value
    * @return
    */
  def aggregateBySession(spark: SparkSession, sessionId2actionRDD: RDD[(String, UserVisitAction)]):RDD[(String,String)] = {
    //对user_visit_action数据表中session进行分组(session,Iterable[UserVisitAction])
    val sessionId2actionsRDD = sessionId2actionRDD.groupByKey()
    val userId2PartAggrInfoRDD = sessionId2actionsRDD.map {
      case (sessionId, userVisitActions) =>

        val searchKeywordBuffer = new StringBuffer()
        val clickCategoryIdsBuffer = new StringBuffer()
        var userId = -1L
        //对session设置起止时间
        var startTime: Date = null
        var endTime: Date = null

        //session访问步长
        var stepLength = 0
        userVisitActions.foreach {
          userVisitAction =>
            if (userId == -1L) {
              userId = userVisitAction.session_id
            }
            val searchKeyword = userVisitAction.search_keyword
            val clickCategoryId = userVisitAction.click_category_id
            //并非所有数据都有查询、点击行为
            //把null值过滤掉
            if(StringUtils.isNotEmpty(searchKeyword)){
              if(!searchKeywordBuffer.toString.contains(searchKeyword)){
                searchKeywordBuffer.append(searchKeyword+",")
              }
            }
            if(clickCategoryId != null && clickCategoryId != -1L){
              if(!clickCategoryIdsBuffer.toString.contains(clickCategoryId)){
                clickCategoryIdsBuffer.append(clickCategoryId+",")
              }
            }

            //计算session开始和结束时间
            val actionTime = DateUtils.parseTime(userVisitAction.action_time)
            if(startTime == null){
              startTime = actionTime
            }
            if(endTime == null){
              endTime = actionTime
            }

            if(actionTime.before(startTime)){
              startTime = actionTime
            }
            if(actionTime.after(endTime)){
              endTime = actionTime
            }

            //计算步长
            stepLength += 1;
        }
        // 截断字符串两侧的逗号
        val searchKeywords = StringUtils.trimComma(searchKeywordBuffer.toString)
        val clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

        //计算访问时长
        val visitLength = (endTime.getTime - startTime.getTime)/1000
        val partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionId+"|"+
          Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords+"|"+
          Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds+"|"
          Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"+
          Constants.FIELD_STEP_LENGTH+"="+visitLength+"|"+
          Constants.FIELD_START_TIME+"="+DateUtils.formatDate(startTime)

        (userId, partAggrInfo)
    }

    import spark.implicits._
    //把user_info用户数据，加载并转换成(userId,Row)格式，图中5
    val userId2InfoRDD = spark.sql("select * from user_info").as[UserInfo].rdd.map(item => (item.user_id, item))

    //把sessionId的聚合数据和用户数据做join
    val userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD)

    val sessionId2FullAggrInfoRDD= userId2FullInfoRDD.map{
      case(uid, (partAggrInfo, userInfo))=>
        val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        val fullAggrInfo = partAggrInfo + "|" +
        Constants.FIELD_AGE+"="+userInfo.age+"|"+
        Constants.FIELD_PROFESSIONAL+"="+userInfo.professional+"|"+
        Constants.FIELD_CITY+"="+userInfo.city+"|"+
        Constants.FIELD_SEX+"="+userInfo.sex+"|"

        (sessionId, fullAggrInfo)
    }
    null
  }

  /**
    * 把表在指定时间内加载出来
    *
    * @param spark
    * @param taskParam
    * @return
    */
  def getActionRDDByDateRange(spark: SparkSession, taskParam: JSONObject):RDD[UserVisitAction] = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    import  spark.implicits._
    spark.sql("select * from user_visit_action where date >= '" + startDate +
      "' and date <= '"+endDate+"'").as[UserVisitAction].rdd
  }

  def main(args: Array[String]): Unit = {
    //获取统计参数
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    //任务ID，用来做唯一表示 ctrl+alt+v
    val taskUUID = UUID.randomUUID().toString
    //构建spark上下文
    val sparkConf = new SparkConf().setAppName("SessionAnalyze").setMaster("local[*]")
    //创建spark客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sparkContext = spark.sparkContext

    //从user_visit_action表中，查询出指定时间[startDate,endDate]范围内的数据
    val actionRDD = this.getActionRDDByDateRange(spark, taskParam)

//    actionRDD.foreach(println(_))
    //把user_visit_action中的数据转化为k,v结构 (sessionId, (date,user_id,session_page_id...))
    val sessionId2actionRDD = actionRDD.map(item => (item.session_id, item))

    //把数据进行内存缓存 查看源码等价 sessionId2actionRDD.cache()
    sessionId2actionRDD.persist(StorageLevel.MEMORY_ONLY)

    //数据转化为session粒度 图中第七步
    val sessionId2AggrInfoRDD = this.aggregateBySession(spark, sessionId2actionRDD)

    spark.close()
  }
}
