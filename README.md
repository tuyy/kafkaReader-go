## toy-project-go 002
 go v1.15.3

### kafkaReader-go
 kafka에 저장되어 있는 특정 토픽의 로그를 아래 조건에 맞게 읽은 후 파일에 저장한다. (복호화 제공 및 history 정보 저장)
 
* startTime ~ endTime
* startOffset ~ endOffset
* limit logCount
* filter text
* kafka key
* kafka headers

#### usage
* config 파일의 우선순위가 커멘드 라인으로 입력한 값보다 높다. (overwrite)
* broker_server, topic 옵션은 필수고 나머지는 기본 값을 사용할 수 있다.
* 종료 조건은, filtered_msg_limit_count, poll_timeout_sec 이다.
* 옵션 기본 값은 아래 링크를 참조한다.
    * [default option value](https://github.com/tuyy/kafkaReader-go/blob/master/pkg/cmd/args.go#L40-L56)
 
 

```
$ ./kafkaReader -b=${broker_servers} \
                -topic=${topic} \
                -partiton=${partiton, default:all} \
                -output=${output_file_path, default) result.log} \
                -start=${start_date_time_str, ex) 202102031421} \
                -end=${end_date_time_str, ex) 202102031423} \
                -startoffset=${start_offset} \
                -endoffset=${end_offset} \
                -limit=${filtered_msg_limit_count} \
                -grep=${included_text} \
                -polltimeout=${poll_timeout_sec} \
                -key=${kafka_key} \
                -headers=${kafka_header_json} \
                -onlymsg=${true_or_false} \
                -decrypted=${payload_decrypt, aes128} \
                -decryptkey=${payload_decrypt_key, default:nvmail} \
                -config=${conf_file_path} \
```

```
// sample 설정 파일은 아래와 같다.
$ cat conf/sample.json
{
  "BrokerServers": [
    "dev-tuyy0-cassandra001-ncl.nfra.io:9092"
  ],
  "Topic": "mytest1",
  "Partition": -1,
  "Key": "",
  "Headers": {},
  "UserName": "",
  "Password": "",
  "StartOffset": 0,
  "EndOffset": 2147483647,
  "Limit": 2147483647,
  "PollTimeout": 10,
  "StartTime": "1991-02-01T00:00:00Z",
  "EndTime": "2041-02-01T00:00:00Z",
  "FilterText": "",
  "Output": "result.log",
  "IsOnlyMsg": true,
  "IsDecrypted": false,
  "DecryptKey": ""
}

# 실행 
$ ls ${HOME_PATH}
cmd  dist  go.mod  go.sum  Makefile  pkg  README.md
$ make build;cd dist
$ ls
conf  kafkaReader  kafkaReaderForCentos
$ ./kafkaReaderForCentos --config=conf/sample.json

==================== BEGIN ====================
:: Broker Servers: [dev-test-kafka001.xfra.io:9092]
:: Topic: mytest1
:: Partition: -1
:: Msg StartTime: 1991/02/01 00:00:00
:: Msg EndTime: 2041/02/01 00:00:00
:: StartOffset: 0
:: EndOffset: 2147483647
:: Filtered Text:
:: Kafka Key:
:: Kafka Header: map[]
:: Filtered Limit: 2147483647
:: Kafka Poll Timeout: 10 sec
:: Payload Decrypted: false
:: Is Only msg: true
=================================================

Waiting.......

=> Stopped Kafka Consumer. because context deadline exceeded.


==================== SUMMARY ====================
:: Total: 1950
:: Filtered: 1950
:: Topic: mytest1
:: Output: result.log
:: Elapsed:10.066 sec
=================================================
```
