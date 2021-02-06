## toy-project-go 002
 go v1.15.3

### kafkaReader-go
 kafka에 저장되어 있는 특정 토픽의 로그를 아래 조건에 맞게 읽은 후 파일에 저장한다.
* startTime ~ endTime
* startOffset ~ endOffset
* limit logCount
* filter text
* kafka key
* kafka headers

#### 로그 형식 예시
```
[2021-02-03 14:21:57.137] CALLER="TEST" CALLER_IP="10.114.148.21"
[2021-02-03 14:22:17.268] CALLER="TEST" CALLER_IP="10.114.128.22"
[2021-02-03 14:23:57.167] CALLER="TEST" CALLER_IP="10.114.128.22"
```

#### usage
```
$ ./kafkaReader -b=${broker_servers} \
                -topic=${topic} \
                -partiton=${partiton, default:all} \
                -output=${output_file_path, default) result.log} \
                -start=${start_date_time_str, ex) 202102031421} \
                -end=${end_date_time_str, ex) 202102031423} \
                -startoffset=${start_offset} \
                -endoffset=${end_offset} \
                -limit=${log_limit_count} \
                -grep=${included_text} \
                -polltimeout=${poll_timeout_sec} \
                -key=${kafka_key} \
                -headers=${kafka_header_json}
                -onlymsg=${true_or_false}
```
