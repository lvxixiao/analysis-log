# 说明

自动创建日志，日志的如下

127.0.0.1 - - Mon May 6 23:47:08 +0800 CST 2019 "OPTIONS /dig?refer=http%3A%2F%2Flocalhost%3A8888%2Fmovie%2F61&time=Mon+May+6+23%3A47%3A08+%2B0800+CST+2019&ua=Mozilla%2F5.0+%28iPhone%3B+U%3B+CPU+iPhone+OS+4_3_2+like+Mac+OS+X%3B+en-us%29+AppleWebKit%2F533.17.9+%28KHTML%2C+like+Gecko%29+Version%2F5.0.2+Mobile%2F8H7+Safari%2F6533.18.5+Quark%2F2.4.2.986&url=http%3A%2F%2Flocalhost%3A8888%2Fmovie%2F61 HTTP/1.1" 200 43 "-" "ua" "-"

包括ip、日期、refer、url、ua。目前版本ip是固定的，日期为当前时间，refer和url是根据ruleResouce()方法返回的resouce来创建url集合并随机选择。可以通过修改该方法来生成自己想要的url。ua则是在全局变量userAgents中随机选取。

# 使用

```
go run main.go -total 100 -filePath ./
```

total:生成的日志行数，默认是100

filePath:日志的存储路径，默认是当前路径(./)
