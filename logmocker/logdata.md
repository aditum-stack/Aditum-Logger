# 日志收集

-------------------------------------------------------------------------------

# INFO级别:

---------------------------------------

## tusi

#### 身份校验请求：
> 身份校验...phone:[phone],imei:[imei]

#### 闸机数据库调用请求：
> AIO_SERVER request...operation:[operation],imei:[imei]

#### 闸机状况：
> 闸机上线请求：try online: [imei]
> 闸机下线请求：try offline: [imei]
> 闸机更新请求：try update: [imei]

#### 闸机数据库操作成功：
> 数据库操作成功

---------------------------------------

## aio

#### 创建新连接：
> 创建新连接 : Connection@{IpAndPort:[ipAndPort],Imei:[imei],session:[session]}

#### 心跳刷新：
> 心跳刷新 : Connection@{IpAndPort:[ipAndPort],Imei:[imei],session:[session]}

#### 新imei替换旧imei：
> 新imei替换旧imei : Connection@{IpAndPort:[ipAndPort],Imei:[imei],session:[session]}

#### 当前连接数：
> [imei]已连接,当前连接数：[this.getConnectNum()]

> [imei]断开连接,当前连接数：[this.getConnectNum()]

#### 远程数据库更新成功：
> 闸机 数据库更新成功 操作:[operation]

#### 接收到数据包：
> 接收到的基础数据为：[dataString]

> imei:[imei]
    
#### 连接事件：
> NEW_SESSION:客户端ID:[sessionId]连接了,当前连接数(已连接)：[bytesHelperInstance.getConnectNum()]

> INPUT_SHUTDOWN:客户端ID:[sessionId]断开连接了,当前连接数：[bytesHelperInstance.getConnectNum()]

> INPUT_EXCEPTION:客户端ID:[sessionId]断开连接了,当前连接数：[bytesHelperInstance.getConnectNum()]

#### 接收远程调用数据包
> AIO_SERVER接收到请求...imei:[imei],operation:[operation]

> 向[imei]回送指令成功：[str]

#### 定时清理死连接：
> -------------clear finish, count : [count]

---------------------------------------    

-------------------------------------------------------------------------------

# WARN级别:

---------------------------------------

## tusi

#### 访问记录生成失败：
> 访问记录生成失败

#### 闸机非法操作：
> error request: [imei]

#### 数据库操作失败：
> 数据库操作失败:[sqlResult]

---------------------------------------

## aio

#### 创建新连接(远程数据库更新失败)：
> 创建新连接(远程数据库更新失败) : Connection@{IpAndPort:[ipAndPort],Imei:[imei],session:[session]}

#### 新imei替换旧imei(远程数据库更新失败)：
> 新imei替换旧imei(远程数据库更新失败) : Connection@{IpAndPort:[ipAndPort],Imei:[imei],session:[session]}

#### 远程数据库更新失败：
> 闸机 数据库更新失败 操作:[operation]

#### 接收数据包错误：
> 数据长度不符合约定，长度为：[data.length]

#### 连接事件：
> INPUT_SHUTDOWN:关闭客户端ID:[sessionId]失败

> INPUT_EXCEPTION:客户端ID:[sessionId]【输入异常】

> INPUT_EXCEPTION:关闭客户端ID:[sessionId]失败

> OUTPUT_EXCEPTION:向客户端ID:[sessionId]【输出异常】

> DEFAULT:处理客户端ID:[sessionId]请求出现异常

#### 接收远程调用数据包
> 向[imei]回送指令失败：[指令]

#### 清理死连接异常：
> -------------clear finish, error : [error]

---------------------------------------

-------------------------------------------------------------------------------

# ERROR级别:

---------------------------------------

## tusi

---------------------------------------

## aio

#### 接收数据包错误
> 数据转换出错

#### 接收数据包错误
> 获取ip、端口信息失败

---------------------------------------