
```shell
# ws连接地址
ws://127.0.0.1:8000/ws?user_id=1

# ws消息格式
{"key":"1","data":"hello"} 
#key：str = user_id, data:any

# http发送消息
curl --location '127.0.0.1:8000/msg' \
--header 'Content-Type: application/json' \
--data '{
    "key":"1",
    "data":"hello"
}'
```