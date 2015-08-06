/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.dsl.views

import java.util.Calendar

import org.schedoscope.dsl.Parameter
import org.schedoscope.dsl.Parameter.p

import org.schedoscope.Settings

import scala.collection.mutable.ListBuffer

object DateParameterizationUtils {
  def earliestDay = Settings().earliestDay

  def parametersToDay(year: Parameter[String], month: Parameter[String], day: Parameter[String]) = {
    val date = Calendar.getInstance()
    date.clear()
    date.set(year.v.get.toInt, month.v.get.toInt - 1, day.v.get.toInt)

    date
  }

  def dayToStrings(thisDay: Calendar) = {
    val year = s"${"%04d".format(thisDay.get(Calendar.YEAR))}"
    val month = s"${"%02d".format(thisDay.get(Calendar.MONTH) + 1)}"
    val day = s"${"%02d".format(thisDay.get(Calendar.DAY_OF_MONTH))}"

    (year, month, day)
  }

  def dayToParameters(thisDay: Calendar) = {
    val year: Parameter[String] = p(s"${"%04d".format(thisDay.get(Calendar.YEAR))}")
    val month: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.MONTH) + 1)}")
    val day: Parameter[String] = p(s"${"%02d".format(thisDay.get(Calendar.DAY_OF_MONTH))}")

    (year, month, day)
  }

  def today = dayToParameters(Settings().latestDay)

  def prevDay(thisDay: Calendar): Option[Calendar] = {
    if (thisDay.after(earliestDay)) {
      val prevDay = thisDay.clone().asInstanceOf[Calendar]
      prevDay.add(Calendar.DAY_OF_MONTH, -1)
      Some(prevDay)
    } else {
      None
    }
  }

  def prevDay(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Option[(String, String, String)] = {
    prevDay(parametersToDay(year, month, day)) match {
      case Some(previousDay) => Some(dayToStrings(previousDay))
      case None => None
    }
  }

  def prevDaysFrom(thisDay: Calendar): Seq[Calendar] = {
    new Iterator[Calendar] {
      var current: Option[Calendar] = Some(thisDay)

      override def hasNext = current != None

      override def next = current match {
        case Some(day) => {
          current = prevDay(day)
          day
        }
        case None => null
      }
    }.toSeq
  }

  def dayRange(fromThisDay: Calendar, toThisDay: Calendar): Seq[Calendar] = {
    new Iterator[Calendar] {
      var current: Option[Calendar] = Some(toThisDay)

      override def hasNext = current != None

      override def next = current match {
        case Some(day) => {
          current = if (current.get.after(fromThisDay)) prevDay(day)
          else
            None
          day
        }
        case None => null
      }
    }.toSeq
  }

  def dayParameterRange(range: Seq[Calendar]): Seq[(String, String, String)] =
    range.map { dayToStrings(_) }

  def monthParameterRange(range: Seq[Calendar]): Seq[(String, String)] =
    range.map { dayToStrings(_) }.map { case (year, month, day) => (year, month) }.distinct

  def dayRangeAsParams(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Seq[(String, String, String)] =
    prevDaysFrom(parametersToDay(year, month, day)).map { dayToStrings(_) }

  def thisAndPrevDays(year: Parameter[String], month: Parameter[String], day: Parameter[String]): Seq[(String, String, String)] =
    prevDaysFrom(parametersToDay(year, month, day)).map { dayToStrings(_) }

  def thisAndPrevDays(year: Parameter[String], month: Parameter[String]): Seq[(String, String, String)] = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3)
  }

  def thisAndPrevMonths(year: Parameter[String], month: Parameter[String]): Seq[(String, String)] = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val lastOfMonthParameters = dayToParameters(lastOfMonth)

    thisAndPrevDays(lastOfMonthParameters._1, lastOfMonthParameters._2, lastOfMonthParameters._3).map { case (year, month, day) => (year, month) }.distinct
  }

  def prevMonth(thisDay: Calendar): Option[Calendar] = {
    if (thisDay.after(earliestDay)) {
      val prevDay = thisDay.clone().asInstanceOf[Calendar]
      prevDay.add(Calendar.MONTH, -1)
      Some(prevDay)
    } else {
      None
    }
  }

  def prevMonth(year: Parameter[String], month: Parameter[String]): Option[(String, String)] = {
    prevMonth(parametersToDay(year, month, p("01"))) match {
      case Some(previousDay) => Some((dayToStrings(previousDay)._1, dayToStrings(previousDay)._2))
      case None => None
    }
  }

  def allDays() = {
    val (todaysYear, todaysMonth, todaysDay) = today
    thisAndPrevDays(todaysYear, todaysMonth, todaysDay)
  }

  def allMonths() = {
    val (todaysYear, todaysMonth, _) = today
    thisAndPrevMonths(todaysYear, todaysMonth)
  }

  def allDaysOfMonth(year: Parameter[String], month: Parameter[String]) = {
    val lastOfMonth = parametersToDay(year, month, p("01"))
    lastOfMonth.add(Calendar.MONTH, 1)
    lastOfMonth.add(Calendar.DAY_OF_MONTH, -1)

    val days = ListBuffer[(String, String, String)]()

    var currentDate = lastOfMonth
    var firstOfMonthReached = false

    while (!firstOfMonthReached) {
      firstOfMonthReached = currentDate.get(Calendar.DAY_OF_MONTH) == 1
      days += dayToStrings(currentDate)
      currentDate.add(Calendar.DAY_OF_MONTH, -1)
    }

    days.toList
  }
}

trait MonthlyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]
  import DateParameterizationUtils._
  val monthId: Parameter[String] = p(s"${year.v.get}${month.v.get}")

  def prevMonth() = DateParameterizationUtils.prevMonth(year, month)

  def thisAndPrevMonths() = DateParameterizationUtils.thisAndPrevMonths(year, month)

  def thisAndPrevDays() = DateParameterizationUtils.thisAndPrevDays(year, month)

  def thisAndPrevDaysUntil(thisDay: Calendar) =
    DateParameterizationUtils.prevDaysFrom(thisDay)

  def allDays() = DateParameterizationUtils.allDays()

  def allMonths() = DateParameterizationUtils.allMonths()

  def lastMonths(c: Int) = {
    val to = parametersToDay(year, month, p("1"))
    val from = to
    from.add(Calendar.MONTH, c)
    monthParameterRange(dayRange(from, to))
  }

  def allDaysOfMonth() = DateParameterizationUtils.allDaysOfMonth(year, month)
}

trait DailyParameterization {
  val year: Parameter[String]
  val month: Parameter[String]
  val day: Parameter[String]
  import DateParameterizationUtils._
  val dateId: Parameter[String] = p(s"${year.v.get}${month.v.get}${day.v.get}")

  def prevDay() = DateParameterizationUtils.prevDay(year, month, day)

  def prevMonth() = DateParameterizationUtils.prevMonth(year, month)

  def thisAndPrevDays() = DateParameterizationUtils.thisAndPrevDays(year, month, day)

  def thisAndPrevMonths() = DateParameterizationUtils.thisAndPrevMonths(year, month)

  def allDays() = DateParameterizationUtils.allDays()

  def allMonths() = DateParameterizationUtils.allMonths()

  def lastMonths(c: Int) = {
    val to = parametersToDay(year, month, day)
    val from = to
    from.add(Calendar.MONTH, c)
    dayParameterRange(dayRange(from, to))
  }

  def lastDays(c: Int) = {
    val to = parametersToDay(year, month, day)
    val from = to
    from.add(Calendar.DATE, c)
    dayParameterRange(dayRange(from, to))
  }
}