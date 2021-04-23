[English version](../en/curve-client-python-api_en.md)

#### 获取一个与curve集群交互的CBDClient对象

```python
import curvefs

# 获取一个与后端集群交互的CBDClient对象，不同的CBDClient对象可以与不同的集群建立连接
cbd1 = curvefs.CBDClient()
cbd2 = curvefs.CBDClient()
```

#### 初始化CBDClient

```python
import curvefs
cbd = curvefs.CBDClient()

# 参数：curve-client配置文件的绝对路径
# 返回值：返回0表示初始化成功，-1表示初始化失败
cbd.Init("/etc/curve/client.conf")
```

#### 创建文件

```python
import curvefs
cbd = curvefs.CBDClient()
cbd.Init("/etc/curve/client.conf")  # 后续示例省略初始化过程

# 参数：三个参数分别为
#      文件全路径
#      文件所属用户信息
#      文件大小
# 返回值：返回0表示创建成功，否则返回错误码

# 首先初始化user信息(curvefs的控制面接口都有用户信息验证，都需要传入用户信息)
user = curvefs.UserInfo_t()
user.owner = "curve"
user.password = ""  # 密码为空时，可以省略

# 调用Create接口创建文件
cbd.Create("/curve", user, 10*1024*1024*1024)

# UserInfo定义如下
typedef struct UserInfo {
    char owner[256];      # 用户名
    char password[256];   # 用户密码
} UserInfo_t;
```

#### 查看文件信息

```python
# 参数：三个参数分别为
#      文件名
#      用户信息
#      文件信息[出参]
# 返回值：返回0表示获取成功，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"

# 构造file信息
finfo = curvefs.FileInfo_t()

# 查看文件信息
cbd.StatFile("/curve", user, finfo)
print finfo.filetype
print finfo.length
print finfo.ctime

# FileInfo定义如下
typedef struct FileInfo {
    uint64_t      id;
    uint64_t      parentid;
    int           filetype;   # 卷类型
    uint64_t      length;     # 卷大小
    uint64_t      ctime;      # 卷创建时间
    char          filename[256];   # 卷名
    char          owner[256];      # 卷所属用户
    int           fileStatus;      # 卷状态
} FileInfo_t;

# 文件状态
#define CURVE_FILE_CREATED            0
#define CURVE_FILE_DELETING           1
#define CURVE_FILE_CLONING            2
#define CURVE_FILE_CLONEMETAINSTALLED 3
#define CURVE_FILE_CLONED             4
#define CURVE_FILE_BEINGCLONED        5
```

#### 扩容文件

```python
# 参数：三个参数分别为
#      文件名
#      用户信息
#      扩容后文件大小
# 返回值：返回0表示扩容成功，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"

# 扩容
cbd.Extend("/curve", user, 20*1024*1024*1024)

# 查看扩容后的文件信息
finfo = curvefs.FileInfo_t()
cbd.StatFile("/curve", user, finfo)
print finfo.length
```

#### 打开/关闭文件

```python
# 打开文件
# 参数：两个参数分别为
#      文件名
#      用户信息
# 返回值：打开成功返回文件fd，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "user1"

# 打开文件，返回fd
fd = cbd.Open("/tmp1", user)

# 关闭文件
# 参数：打开文件时返回的fd
# 返回值：关闭成功返回0，否则返回错误码
cbd.Close(fd)
```

#### 读写文件

```python
# 写文件
# 参数：四个参数分别为
#      文件fd
#      待写入数据
#      偏移量
#      写入数据长度
# 返回值：写入成功返回写入字节数，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "user1"

# 打开文件，返回fd
fd = cbd.Open("/tmp1", user)

# 写文件(目前读写都需要4k对齐)
cbd.Write(fd, "aaaaaaaa"*512, 0, 4096)
cbd.Write(fd, "bbbbbbbb"*512, 4096, 4096)

# 读文件
# 参数：四个参数分别为
#      文件fd
#      空字符串
#      偏移量
#      读取数据长度
# 返回值：读取成功返回读取数据，否则返回错误码

# 读取的内容通过返回值返回，buf在此没有意义，可以传入一个空串
cbd.Read(fd，"", 0, 4096)

# 关闭文件
cbd.Close(fd)
```

备注：当前python api接口，不支持异步读写

#### 删除文件

```python
# 参数：两个参数分别为
#      文件名
#      用户信息
# 返回值：删除成功返回0，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"

# 删除文件
cbd.Unlink("/curve", user)
```
#### 恢复文件

```python
# 参数：三个参数分别为
#      文件名
#      用户信息
#      文件id（可选，默认为0）
# 返回值：恢复成功返回0，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"

# 恢复文件
cbd.Recover("/curve", user, 0)
```

#### 重命名文件

```python
# 参数：三个参数分别为
#      用户信息
#      旧文件名
#      新文件名
# 返回值：成功返回0，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"

# 重命名
cbd.Rename(user, "/curve", "/curve-new")
```

#### 创建目录

```python
# 参数：两个参数分别为
#      目录路径
#      用户信息
# 返回值：成功返回0，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"
# 创建目录
cbd.Mkdir("/curvedir", user)
```

#### 删除目录

```python
# 参数：两个参数分别为
#      目录路径
#      用户信息
# 返回值：成功返回0，否则返回错误码

# 构造user信息
user = curvefs.UserInfo_t()
user.owner = "curve"

# 删除目录
cbd.Rmdir("/curvedir", user)
```

#### 获取目录下的文件

```python
# 参数：两个参数分别为
#      目录路径
#      用户信息
# 返回值：当前目录下的文件列表(只包括文件名)
files = cbd.Listdir("/test", user)
for f in files:
    print f
```

#### 获取集群ID

```python
# 通过返回值判断是否获取成功
# 成功返回集群id字符串
# 失败返回空字符串
clusterId = cbd.GetClusterId()
print clusterId
# c355675a-f4d2-4729-b80a-5a7bcc749d1c
```

#### 清理CBDClient对象

```python
cbd.UnInit()
```

### 错误码

| Code | Message                     | 描述                     |
| :--: | :-------------------------- | ------------------------ |
|  0   | OK                          | 操作成功                 |
|  -1  | EXISTS                      | 文件或目录已存存在       |
|  -2  | FAILED                      | 操作失败                 |
|  -3  | DISABLEDIO                  | 禁止IO                   |
|  -4  | AUTHFAIL                    | 认证失败                 |
|  -5  | DELETING                    | 正在删除                 |
|  -6  | NOTEXIST                    | 文件不存在               |
|  -7  | UNDER_SNAPSHOT              | 快照中                   |
|  -8  | NOT_UNDERSNAPSHOT           | 非快照状态               |
|  -9  | DELETE_ERROR                | 删除错误                 |
| -10  | NOT_ALLOCATE                | Segment未分配            |
| -11  | NOT_SUPPORT                 | 操作不支持               |
| -12  | NOT_EMPTY                   | 目录非空                 |
| -13  | NO_SHRINK_BIGGER_FILE       | 禁止缩容                 |
| -14  | SESSION_NOTEXISTS           | Session不存在            |
| -15  | FILE_OCCUPIED               | 文件被占用               |
| -16  | PARAM_ERROR                 | 参数错误                 |
| -17  | INTERNAL_ERROR              | 内部错误                 |
| -18  | CRC_ERROR                   | CRC检查错误              |
| -19  | INVALID_REQUEST             | 请求参数存在异常         |
| -20  | DISK_FAIL                   | 磁盘异常                 |
| -21  | NO_SPACE                    | 空间不足                 |
| -22  | NOT_ALIGNED                 | IO未对齐                 |
| -23  | BAD_FD                      | 文件正在被关闭，fd不可用 |
| -24  | LENGTH_NOT_SUPPORT          | 文件长度不满足要求       |
| -25  | SESSION_NOT_EXIST           | Session不存在(与-14重复) |
| -26  | STATUS_NOT_MATCH            | 状态异常                 |
| -27  | DELETE_BEING_CLONED         | 删除文件正在被克隆       |
| -28  | CLIENT_NOT_SUPPORT_SNAPSHOT | Client版本不支持快照     |
| -29  | SNAPSHOT_FROZEN             | Snapshot功能禁用中       |
| -100 | UNKNOWN                     | 未知错误                 |

