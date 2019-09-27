package com.air.antispider.stream.preprocess.encrypted

import java.util.regex.{Matcher, Pattern}

import com.air.antispider.stream.common.bean.Message
import com.air.antispider.stream.common.util.decode.MD5

object EncryptedData {

  // 加密手机号
  def encryptedPhone(message: Message): Message = {

    // 获取cookie信息  -> 手机号在cookie里面
    var cookie: String = message.http_cookie.toString

    // 1. 获取加密对象
    val md5 = new MD5

    // 2. 获取正则对象
    val pattern: Pattern = Pattern.compile("((13[0-9])|(14[5|7])|(15([0-3]|[5-9]))|(17[0-9])|(18[0,5-9]))\\d{8}")

    // 3. 使用正则匹配字符串,返回match -> 匹配到多少个都封装到match对象里面

    val matcher: Matcher = pattern.matcher(cookie)

    // 4. 判断matcher里面有没有值 -> 有值就取出来加密
    while (matcher.find()) {

      // a. 获取匹配到的数据
      val phoneNum: String = matcher.group()

      // b. 加密
      cookie = cookie.replace(phoneNum, md5.getMD5ofStr(phoneNum))

      // e. 将cookie设置回去
      message.setHttp_cookie(cookie)

    }
    // f. 返回message
    message
  }

  // 加密身份证号 -> 加密的数据是手机号解密之后的数据
  def encryptedID(encryptedPhoneMessage: Message): Message = {

    // 将message转化为string -> 数据中没有身份账号
    var cookie: String = encryptedPhoneMessage.getHttp_cookie

    // 1. 获取加密对象
    val md5 = new MD5

    // 2. 编译手机号正则
    val pattern: Pattern = Pattern.compile("(\\\\d{18})|(\\\\d{17}(\\\\d|X|x))|(\\\\d{15})")

    // 3. 使用正则匹配字符串,返回match -> 匹配到多少个都封装到match对象里面

    val matcher: Matcher = pattern.matcher(cookie)

    // 4. 判断matcher里面有没有值 -> 有值就取出来加密
    while (matcher.find()) {

      // a. 获取匹配到的数据
      val phoneNum: String = matcher.group()

      // b. 加密
      cookie = cookie.replace(phoneNum, md5.getMD5ofStr(phoneNum))

      // e. 将cookie设置回去
      encryptedPhoneMessage.setHttp_cookie(cookie)

    }
    // e. 返回message
    encryptedPhoneMessage
  }

}
