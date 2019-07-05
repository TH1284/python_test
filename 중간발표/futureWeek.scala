package com.haiteam


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row

object futureWeek {
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

    /////////////////////////  빈주차 생성시 사용되는 함수 끝~~ ////////////////////////////////////


    var targetFile = "pro_actual_sales.csv"

    // 절대경로 입력
    var salesData =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + targetFile)

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

    val resultRdd = selloutRdd.groupBy(x => { // 그룹바이
      (
        x.getString(noRegionSeg1), // 미국
        x.getString(noProductSeg1), // 널값
        x.getString(noProductSeg2), // 냉장고
        x.getString(noRegionSeg2), // 이마트
        x.getString(noRegionSeg3), // 플로리다
        x.getString(noProductSeg3) // 냉장고001
      )
    }).flatMap(f = x => {
      val key = x._1 // 위 그룹바이 한 애들이 키값으로 들어가 있음
      val data = x._2 // 디버깅 할때 data.head 로 한줄 확인 가능

      // Using Difference Set
      val yearweekArray = data.map(x => { // data 중에서 noYearweek 해당하는 애들만 뽑아서 배열에 넣겟다!
        x.getString(noYearweek)
      }).toArray

      var minYearweek: String = yearweekArray.min // 201517  , min 값 구함
      var maxYearweek: String = yearweekArray.max // 201627  , max 값 구함

      var fullYearweek = Array.empty[String] // 빈배열을 만듬
      var currentYearweek = "" // 빈 스트링 만듬
      var gapWeek = 0 // gapWeek 는 0

      do {
        currentYearweek = postWeek(minYearweek, gapWeek) // 그룹단위 minyearweek, 갭주차 0
        fullYearweek ++= Array(currentYearweek) // 위 postWeek 의 리턴값을 currentYearweek 에 담고 위에서 만든 빈 배열에 더해줌
        gapWeek += 1 // gapWeek 를 1씩 증가시킴
      } while (currentYearweek != maxYearweek) // 해당 조건식이 true 면 시작 false 면 끝
      val emptyYearweek: Array[String] = fullYearweek.diff(yearweekArray) // 201519 , 201624

      val emptyMap = emptyYearweek.map(yearweek => { // 비어있는 emptyYearweek 을 map 연산을 함
        val year = yearweek.substring(0, 4)
        val week = yearweek.substring(4)
        val qty = 0.0d // 비어 있는 emptyYearweek 에 대해서 qty 값이 없기 때문에 0으로 채워줌
        Row( // Row 를 사용하면 아래 data 와 emptyMap 더해줄때 타입을 emptyMap.toIterable 식으로 바꿔줘야함
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨. -> 맞나 체크
          yearweek, // 비어있는 yearweek 가 들어옴
          year, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          week, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          qty.toString, // 위에서 0으로 채워준거 적어줌
          "0"
          //output : (noProductSeg1, noProductSeg2, noRegionSeg2, noRegionSeg3, noProductSeg3, noProductSeg3
        )
      })
      ////////////////////////////////////  미래주차 생성 ////////////////////////////////////

      var fullYearweek2 = Array.empty[String] // 빈배열을 만듬
      var currentYearweek2 = "" // 빈 스트링 만듬
      var gapWeek2 = 1 // gapWeek 는 0
      var postWeek_input = 4 // 최대 주차 + 4주차까지 구하겟다!
      var futureYearweek = postWeek(maxYearweek, postWeek_input) // 함수를 이용해서!

      do {
        currentYearweek2 = postWeek(maxYearweek, gapWeek2) // 그룹단위 maxYearweek
        fullYearweek2 ++= Array(currentYearweek2) // 위 postWeek 의 리턴값을 currentYearweek 에 담고 위에서 만든 빈 배열에 더해줌
        gapWeek2 += 1 // gapWeek 를 1씩 증가시킴
      } while (currentYearweek2 != futureYearweek) // 해당 조건식이 true 면 시작 false 면 끝
      val emptyYearweek2: Array[String] = fullYearweek2 // 201519 , 201624

      val fcstRow = emptyYearweek2.map(yearweek => { // 비어있는 emptyYearweek 을 map 연산을 함
        val fcstyear = yearweek.substring(0, 4)
        val fcstweek = yearweek.substring(4)
        val fcstqty = 0.0d // 비어 있는 emptyYearweek 에 대해서 qty 값이 없기 때문에 0으로 채워줌
        var fcst = 0.0d  // fcst 컬럼 추가
        Row( // Row 를 사용하면 아래 data 와 emptyMap 더해줄때 타입을 emptyMap.toIterable 식으로 바꿔줘야함
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6, // 여기까진 그룹바이 했기 때문에 키값으로 넣어주면 됨. -> 맞나 체크
          yearweek, // 비어있는 yearweek 가 들어옴
          fcstyear, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          fcstweek, // 비어있는 yearweek 를 바로 위에서 substring 한 값
          fcstqty.toString, // 위에서 0으로 채워준거 적어줌
          fcst.toString
          //output : (noProductSeg1, noProductSeg2, noRegionSeg2, noRegionSeg3, noProductSeg3, noProductSeg3
        )
      })

      /////////////////////////////////////  미래주차 생성 ////////////////////////////////////
      val originMap = data.map(x=>{
        Row(
          key._1,
          key._2,
          key._3,
          key._4,
          key._5,
          key._6,
          x.getString(noYearweek),
          x.getString(noYear),
          x.getString(noWeek),
          x.get(noQty).toString.toDouble,
          "0"
        )
      })

      originMap ++ emptyMap.toIterable ++ fcstRow.toIterable // 모든 data 에 emptyMap (비어있는 yearweek 에 대해 설정한 놈) 을 더해줌
    }).filter(x => { //
      x.getString(noWeek).toInt <= 52 // row 형태면 getString 으로 가져오고, row 안쓰면 시리즈였나 그 상태면 x._7 으로 가져와야함
    })

    // resultRdd.take(68).foreach(println)

  }
}
