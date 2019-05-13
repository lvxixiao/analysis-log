# 说明

解析网站日志，并将结果(pv、uv)存入redis。

日志生成使用这个项目[https://github.com/lvxixiao/create-log](https://github.com/lvxixiao/create-log)

# 使用

```
go run main.go -logFilePath ./log.txt" -rutineNume 3 -l ./temp/analysisLog.txt
```

-logFilePath: 要解析的日志文件，默认是当前目录下的log.txt

-rutineNume：设置gorutine的并发度

-l：该程序运行时产生的日志保存地址，默认是当前目录下的temp字子目录analysisLog.txt文件

