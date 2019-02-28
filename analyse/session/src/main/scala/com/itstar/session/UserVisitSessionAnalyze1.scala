package com.itstar.session

import java.util.{Date, Random, UUID}

import com.itstar.commons.conf.ConfigurationManager
import com.itstar.commons.constant.Constants
import com.itstar.commons.model.{UserInfo, UserVisitAction}
import com.itstar.commons.utils.{DateUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
              userId = userVisitAction.user_id
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

  def filterSessionAndAggrStat(sessionId2AggrInfoRDD: RDD[(String, String)],
                               taskParam: JSONObject,
                               sessionAggrStatAccumulator: AccumulatorV2[String, mutable.HashMap[String, Int]] ):RDD[(String,String)] = {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywrods = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.FIELD_CLICK_CATEGORY_IDS)

    var _paramter = ""
    if(_paramter.endsWith("\\|")){
      _paramter = _paramter.substring(0, _paramter.length-1)
    }
    val paramter = _paramter

    val filteredSession2AggrInfoRDD = sessionId2AggrInfoRDD.filter{
      case (sessionId, aggrInfo) =>
        //按照年龄范围进行过滤（startAge，endAge）
        var success = true
        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, paramter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false


        if(success){
          sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
          def calculateVisitLength(visitLength:Long):Unit={
            if(visitLength >=1 && visitLength <= 3){
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if(visitLength >=4 && visitLength <= 6){
              sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            }

          }

          def calculateStepLength(stepLength:Long):Unit={
            if(stepLength >=1 && stepLength <= 3){
              sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
            }
          }


          val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength)
          calculateStepLength(stepLength)
        }
        success
    }

    filteredSession2AggrInfoRDD
  }

  /**
    * session范围比，写入mysql
    * @param spark
    * @param value
    * @param taskUUID
    */
  def calculateAndPersistAggrStat(spark: SparkSession,
                                  value: mutable.HashMap[String, Int],
                                  taskUUID: String) = {
    //SessionAggrStatAccumulator从累加器中获得值
    val session_count = value(Constants.SESSION_COUNT).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)

    //将统计结果封装
    val sessionAggrStat = SessionAggrStat(taskUUID)

    import spark.implicits._
    val sessionAggrStatRDD = spark.sparkContext.makeRDD(Array(sessionAggrStat))

    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat")
      .option("user", "")
      .option("password", "")
      .mode(SaveMode.Append)
  }

  /**
    * 通过筛选条件的session的访问明细数据
    *
    * @param filteredSession2AggrInfoRDD 过滤RDD
    * @param sessionId2actionRDD （K，V）结构user_visit_action RDD
    *
    */
  def getSessionId2detailRDD(filteredSession2AggrInfoRDD: RDD[(String, String)],
                             sessionId2actionRDD: RDD[(String, UserVisitAction)]) = {
    filteredSession2AggrInfoRDD.join(sessionId2actionRDD)
  }

  /**
    * 需求二：随机抽取session
    *
    * @param spark
    * @param taskUUID
    * @param filteredSession2AggrInfoRDD
    * @param sessionId2detailRDD
    */
  def randomExtractSession(spark: SparkSession,
                           taskUUID: String,
                           filteredSession2AggrInfoRDD: RDD[(String, String)],
                           sessionId2detailRDD: RDD[(String, (String, UserVisitAction))]) = {
    val time2sessionRDD = filteredSession2AggrInfoRDD.map{
      case (sessionId, fullAggrInfo) =>
        //startTime <yyyy-MM-dd HH:mm:ss>
        val startTime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME)
        //
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullAggrInfo)
    }
    // <yyyy-MM-dd_HH, count>
    val countMap = time2sessionRDD.countByKey()

    //
    val dateHourCountMap = mutable.HashMap[String, mutable.HashMap[String, Long]]

    for((dateHour, count) <- dateHourCountMap){
      val date = dateHour.split("_")[0]
      val hour = dateHour.split("_")[1]

      dateHourCountMap.get(date) match {
        case None =>
          dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(hourCountMap) =>
          hourCountMap += (hour -> count)
      }

    }

    //按时间比例随机抽取session，假设要100个，先按天分
    //获得每天要抽取的数据量
    //
    val extractNumberPerDay = 100 / dateHourCountMap.size

    //[天，[小时，()]]
    val dateHourExtractMap = mutable.HashMap[String,mutable.HashMap[String,mutable.ListBuffer[Int]]]()
    val random = new Random()

    /**
      * 每个小时应该抽取的数据，来产生随机值
      * 遍历每小时，去填充
      *
      * @param hourExtractMap
      * @param hourCountMap
      * @param sessionCount
      */
    def hourExtractMapFunc(hourExtractMap:mutable.HashMap[String,mutable.ListBuffer[Int]],
                           hourCountMap:mutable.HashMap[String,Long],
                           sessionCount:Long): Unit = {
      for((hour,count) <- hourCountMap){
        //计算每小时的session数量=当前总session的比例*每天要抽取的数量
        val hourExtractNumber = ((count / sessionCount.toDouble) * extractNumberPerDay).toInt
        if(hourExtractNumber > count){
          hourExtractNumber = count.toInt
        }

        //模式匹配：
        hourExtractMap.get(hour) match {
          case None =>
            hourExtractMap(hour) = new mutable.ListBuffer[Int]()
            for(i <- 0 to hourExtractNumber){
              val extractIndex = random.nextInt(i.toInt)
              while(hourExtractMap(hour).contains(i.toInt)){
                extractIndex = random.nextInt(i.toInt)
              }
              hourExtractMap(hour) += extractIndex
            }

          case Some(extractIndexList) =>

        }
      }
    }


    //session随机抽取功能
    for((date,hourCountMap) <- dateHourCountMap){
      //一天的session总量
      val sessionCount = hourCountMap.values.sum

      dateHourExtractMap.get(date) match {
        case None =>
          dateHourExtractMap(date) = new mutable.HashMap[String,mutable.ListBuffer[Int]]()
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, sessionCount)
        case Some(hourCountMap) =>
          hourExtractMapFunc(dateHourExtractMap(date), hourCountMap, sessionCount)

      }
    }

    val dateHourExtractMapBroadcast = spark.sparkContext.broadcast(dateHourExtractMap)

    val time2sessionsRDD = time2sessionRDD.groupByKey()

    //第三步：把每天每小时的session，根据索引抽取
    val sessionRandomExtract = time2sessionsRDD.flatMap{
      case (dateHour, items) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)

        val dateHourExtractMap = dateHourExtractMapBroadcast.value

        val exterIndexList = dateHourExtractMap.get(date).get(hour)
        var index = 0
        val SessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
        for(sessionAggrInfo <- items){
          if(exterIndexList.contains(index)){
            val sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = null;
            val searchKeywords = null;
            val clickCategoryIds = null;

            SessionRandomExtractArray += SessionRandomExtract(taskUUID,sessionId,startTime,searchKeywords,clickCategoryIds)
          }
          index += 1
        }
        SessionRandomExtractArray
    }


    import spark.implicits._
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .save()


    val extractSessionIdsRDD = sessionRandomExtract.map(item => (item.sessionid, item.sessionid))
    //session与详细数据做聚合join
    val extractSessionDetailRDD = extractSessionIdsRDD.join(sessionId2detailRDD)

    val sessionDetailRDD = extractSessionDetailRDD.map{
      case (sid, (sessionId,userVisitAction: UserVisitAction)) =>
        SessionDetail(taskUUID,
          userVisitAction.user_id,
          userVisitAction.session_id,
          userVisitAction.page_id,
          userVisitAction.action_time,
          userVisitAction.search_keyword,
          userVisitAction.click_category_id,
          userVisitAction.click_product_id,
          userVisitAction.order_category_ids,
          userVisitAction.order_product_ids,
          userVisitAction.pay_category_ids,
          userVisitAction.pay_product_ids)
    }
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .save()


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

//    sessionId2AggrInfoRDD.foreach(println(_))
    //自定义累加器
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    //注册累加器
    sparkContext.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")
    //过滤数据集
    val filteredSession2AggrInfoRDD = filterSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator)
//    filteredSession2AggrInfoRDD.foreach(println(_))
    filteredSession2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY)

    //需求一：统计各个范围session范围占比，写入mysql
    calculateAndPersistAggrStat(spark, sessionAggrStatAccumulator.value, taskUUID)


    val sessionId2detailRDD = getSessionId2detailRDD(filteredSession2AggrInfoRDD, sessionId2actionRDD)
    sessionId2detailRDD.cache()
    //需求二：随机均匀获取session
    randomExtractSession(spark, taskUUID, filteredSession2AggrInfoRDD, sessionId2detailRDD)

    spark.close()
  }
}
