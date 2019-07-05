package com.haiteam


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

object seasonality {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc)


    /////////////////////////  빈주차 생성시 사용되는 함수 ////////////////////////////////////


    def getLastWeek(year: Int): Int = {  // Lastweek 가 언제인지 알려주는 함수

      val calendar: java.util.Calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      val dateFormat: java.text.SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      calendar.setTime(dateFormat.parse(year + "1231"))

      return calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
    }

    def postWeek(inputYearWeek: Any, gapWeek: Any): String = {  //
      var strInputYearWeek: String = inputYearWeek.toString()  // substring 해주기 위해 String 으로 형 변환
    var strGapWeek: String = gapWeek.toString() // substring 해주기 위해 String 으로 형 변환

      var inputYear: Int = Integer.parseInt(strInputYearWeek.substring(0, 4))  // 2015  다시 int 형으로
      var inputWeek: Int = Integer.parseInt(strInputYearWeek.substring(4))  //  17
      var inputGapWeek: Int = Integer.parseInt(strGapWeek)  // string 형인 strGapWeek 도 int 형으로

      var calcWeek: Int = inputWeek + inputGapWeek.abs   // 17 + 0 (1씩 증가함) abs 는 왜..?

      while(calcWeek > getLastWeek(inputYear)) {  // 17 > 52  ( 2015년의 라스트주차 예를들어 52 )
        calcWeek -= getLastWeek(inputYear)
        inputYear += 1
      }

      var resultYear: String = inputYear.toString()  // 2015 가 resultYear 에 담김
      var resultWeek: String = calcWeek.toString()  // 17 resultWeek 에 담김

      if (resultWeek.length < 2) {
        resultWeek = "0" + resultWeek
      }
      return resultYear + resultWeek  // 2015 + 17
    }

    /////////////////////////////////////////////////////////////////////////////

    // Step1. 데이터 로드

    var targetFile = "pro_actual_sales.csv"

    // 절대경로 입력
    var salesData =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + targetFile)

    // 컬럼 인덱스 재정의
    val salesColumns = salesData.columns
    val noRegionSeg1 = salesColumns.indexOf("regionSeg1") // ex 미국
    val noProductSeg1 = salesColumns.indexOf("productSeg1") // 널값
    val noProductSeg2 = salesColumns.indexOf("productSeg2") // 냉장고
    val noRegionSeg2 = salesColumns.indexOf("regionSeg2") // 이마트
    val noRegionSeg3 = salesColumns.indexOf("regionSeg3") // 플로리다
    val noProductSeg3 = salesColumns.indexOf("productSeg3") // 냉장고001 ( 냉장고 제품번호 )
    val noYearweek = salesColumns.indexOf("yearweek")
    val noYear = salesColumns.indexOf("year")
    val noWeek = salesColumns.indexOf("week")
    val noQty = salesColumns.indexOf("qty")

    val selloutRdd = salesData.rdd


    // Step2. 데이터 정제,
    // Step.2-1. 빈주차 생성

    val resultRdd = selloutRdd.groupBy(x => {
      (
        x.getString(noRegionSeg1), // 미국
        x.getString(noProductSeg1), // 널값
        x.getString(noProductSeg2), // 냉장고
        x.getString(noRegionSeg2), // 이마트
        x.getString(noRegionSeg3), // 플로리다
        x.getString(noProductSeg3) // 냉장고001
      )
    }).flatMap(x => {
      val key = x._1 // 위 groupBy 한 애들이 키값으로 들어가 있음
      val data = x._2 // 디버깅 할때 data.head 로 한줄 확인 가능

      // Using Difference Set, 차집합
      val yearweekArray = data.map(x => { // data 중에서 noYearweek 해당하는 애들만 뽑아서 배열에 넣겟다!
        x.getString(noYearweek)
      }).toArray

      var minYearweek: String = yearweekArray.min // 201517  , min 값 구함
      var maxYearweek: String = yearweekArray.max // 201627  , max 값 구함

      var fullYearweek = Array.empty[String] // 빈배열을 만듬
      var currentYearweek = "" // 빈 스트링 만듬
      var gapWeek = 0 // gapWeek 는 0

      do {
        currentYearweek = postWeek(minYearweek, gapWeek) // 그룹단위로 minyearweek, 갭주차 0  (201517,0)
        fullYearweek ++= Array(currentYearweek) // 위 postWeek 의 리턴값을 currentYearweek 에 담고 위에서 만든 빈 배열에 더해줌
        gapWeek += 1 // gapWeek 를 1씩 증가시킴
      } while (currentYearweek != maxYearweek) // 해당 조건식이 true 면 시작 false 면 끝
      val emptyYearweek: Array[String] = fullYearweek.diff(yearweekArray) // 201519 , 201624

      //      var emptySize = emptyYearweek.size   // 이런 방법도 있다. 하지만 속도는 map 에 비해 느리다, 하지만 이친구를 써야할 때가 있다.
      //      var emptyArrayBuffer = Array.fill(emptySize)("rseg1","pseg1","pseg2","rseg2","rseg3","pseg3","yearweek","year","week","qty")
      //
      //      var i = 0
      //      while(i<emptySize){
      //          emptyArrayBuffer(i) = (
      //            key._1,
      //            key._2,
      //            key._3,
      //            key._4,
      //            key._5,
      //            key._6,
      //            emptyYearweek(i),
      //            emptyYearweek(i).substring(0,4),
      //            emptyYearweek(i).substring(4,6),
      //            "0.0"
      //          )
      //          i=i+1
      //        }

      val emptyMap = emptyYearweek.map(yearweek => { // 누락된주차 (emptyYearweek) 을 map 연산을 함
        val year = yearweek.substring(0, 4)
        val week = yearweek.substring(4)
        val qty = 0.0d // 비어 있는 emptyYearweek 에 대해서 qty 값이 없기 때문에 0으로 채워줌
        Row( // Row 를 사용하면 아래 data 와 emptyMap 더해줄때 타입을 emptyMap.toIterable 식으로 바꿔줘야함
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨.
          yearweek, // 비어있는 yearweek 가 들어옴
          year, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          week, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          qty.toString // 위에서 0으로 채워준거 들어감
          // 가독성을 위해
          //output : (noProductSeg1, noProductSeg2, noRegionSeg2, noRegionSeg3, noProductSeg3, noProductSeg3)
        )
      })
      // Step.2-2. 53주차 제거
      data ++ emptyMap.toIterable // 모든 data 에 emptyMap (비어있는 yearweek 에 대해 설정한 놈) 을 더해줌
    }).filter(x => { //
      x.getString(noWeek).toInt <= 52 // row 형태면 getString 으로 가져오고, row 안쓰면 시리즈였나 그 상태면 x._7 으로 가져와야함
    })

    // Step3. 각 년도의 주차별 seasonality 산출

    var finalRdd = resultRdd.groupBy(x=>{
      (x.getString(noRegionSeg1),  // 미국
        x.getString(noProductSeg2),  // 냉장고
        x.getString(noRegionSeg3) // 플로리다
        //        x.getString(noRegionSeg2)
        //        x.getString(noProductSeg3)
        //        x.getString(noYearweek)
      )
    }).flatMap(x=>{
      var key = x._1
      var data = x._2
      var size = data.size

      var qtyList = data.map(x=>{ // QTY 에 해당하는놈만 qtyList 담는다
        x.getString(noQty).toDouble
      })
      // var qtyList = data.map(x=>{x.get(noQty).toString.toDouble})
      // 처음에 어떤 데이터타입이 들어올지 모르기 때문에, get 을 사용후, 원하는 데이터 산출을 위한 타입 캐스팅
      // 이렇게 하면 편하긴 하지만, 속도가 느리다는 단점이 있다.
      var qtyArray = qtyList.toArray
      var qtySum = qtyArray.sum  // QTY 총합을 구함
      var qtyMean = if(size != 0){
        qtySum/size  // QTY 평균을 구함
      }else{
        0.0  // 분모가 0이면 0으로 해준다, 예외처리
      }
      var stdev = if(size != 0){
        Math.sqrt(qtyList.map(x=>{  //qtyList => 각 qty  qtyMean => 그룹바이 qty 합친것에 평균
          Math.pow(x - qtyMean, 2)  // 표준편차
        }).sum/size)
      }else{
        0.0
      }
      var outputData = data.map(x=>{
        var org_qty = x.getString(noQty).toDouble
        var seasonality = if(qtyMean != 0){
          org_qty/qtyMean  // 각 qty 를 qty 총합의 평균으로 나워줌 -> 계절성지수다!
        }else{
          0.0
        }
        (x.getString(noRegionSeg1),   // 미국
          x.getString(noRegionSeg3),  // 플로리다
          x.getString(noRegionSeg2),   // 이마트 ~
          x.getString(noProductSeg1),  // 널값
          x.getString(noProductSeg2),  // 냉장고
          x.getString(noProductSeg3),  // 냉장고001 ( 냉장고 제품번호 )
          x.getString(noYearweek),
          x.getString(noYear),
          x.getString(noWeek),
          x.getString(noQty),
          size,  //
          qtyMean,  // qty 의 총합 / size
          stdev,  //  표준편차
          seasonality)
      })
      outputData
    })

//    finalRdd.take(10).foreach(println)

//    // 새로운 컬럼을 정의 해준다 여기서 컬럼의 이름은 finalRdd의 outputData값으로 #갯수도 일치 해야함(컬럼의갯수)
//    var finalResult = finalRdd.toDF("REGIONSEG1","REGIONSEG2","REGIONSEG3","PRODUCTSEG1","PRODUCTSEG2","PRODUCTSEG3","YEARWEEK","YEAR","WEEK","QTY","SIZE","QTY_MEAN","STDEV","SEASONALITY")
//
//    finalResult.show
//    // 저장단계
//    finalResult.
//      coalesce(1). // 파일개수
//      write.format("csv").  // 저장포맷
//      mode("overwrite"). // 저장모드 append/overwrite
//      option("header", "true"). // 헤더 유/무
//      save("c:/spark/bin/data/pro_actual_sales_season222.csv") // 저장파일명

  }
}
