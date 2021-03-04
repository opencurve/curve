[中文版](../cn/curve-client-python-api.md)

#### Get a CBDClient object that interacts with the Curve cluster

```python
import curvefs

# Get a CBDClient object that interacts with the back-end cluster. Different CBDClient objects can establish connections with different clusters
cbd1 = curvefs.CBDClient()
cbd2 = curvefs.CBDClient()
```

#### CBDClient initialization

```python
import curvefs
cbd = curvefs.CBDClient()
  
# parameter: full path of curve-client configuration file
# return: return 0 if initialized successfully, -1 means failed
cbd.Init("/etc/curve/client.conf")
```

#### Create file

```python
import curvefs
cbd = curvefs.CBDClient()
cbd.Init("/etc/curve/client.conf")  # subsequent examples omit the initialization process
 
# parameter: the three parameters are
#      file full path
#      user information
#      file size
# return: return 0 if create successfully, otherwise return an error code

# First initialize user information (the control plane interfaces of curvefs have user information verification, and user information needs to be passed in)
user = curvefs.UserInfo_t()
user.owner = "curve"
user.password = ""  # when the password is empty, it can be omitted

# Create a file with Create interface
cbd.Create("/curve", user, 10*1024*1024*1024)
  
# UserInfo definition
typedef struct UserInfo {
    char owner[256];       # username 
    char password[256];    # password
} UserInfo_t;
```

#### Get file information

```python
# parameter: the three parameters are
#      filename
#      user information
#      fileInfo[exit parameter]
# return: return 0 if get successfully, otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"

# Construct file information
finfo = curvefs.FileInfo_t()

# Get file information
cbd.StatFile("/curve", user, finfo)
print finfo.filetype
print finfo.length
print finfo.ctime
  
# FileInfo definition
typedef struct FileInfo {
    uint64_t      id;          
    uint64_t      parentid;
    int           filetype;   # volume type
    uint64_t      length;     # volume size
    uint64_t      ctime;      # volume create time
    char          filename[256];   # volume name
    char          owner[256];      # volume owner
    int           fileStatus;      # volume status
} FileInfo_t;
 
# File status
#define CURVE_FILE_CREATED            0
#define CURVE_FILE_DELETING           1
#define CURVE_FILE_CLONING            2
#define CURVE_FILE_CLONEMETAINSTALLED 3
#define CURVE_FILE_CLONED             4
#define CURVE_FILE_BEINGCLONED        5
```

#### Extend file

```python
# parameter: the three parameters are
#      filename
#      user information
#      file size after extend
# return: return 0 if extend successfully, otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"

# Extend
cbd.Extend("/curve", user, 20*1024*1024*1024)

# Get file information after extension
finfo = curvefs.FileInfo_t()
cbd.StatFile("/curve", user, finfo)
print finfo.length
```

#### Open/Close file

```python
# Open file
# parameter: the two parameters are
#      filename
#      user information
# return: return fd if open successfully, otherwise return error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "user1"

# Open file，return fd
fd = cbd.Open("/tmp1", user)
  
# Close file
# parameter: fd returned when opening the file
# return: return 0 if closed successfully, otherwise return an error code
cbd.Close(fd)
```

#### Read and write file

```python
# Write file
# parameter: the four parameters are
#      fd
#      data to write
#      offset
#      date length
# return: return the number of bytes written If write successful, otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "user1"

# Open file，return fd
fd = cbd.Open("/tmp1", user)

# Write files (Currently, 4k alignment is required for reading and writing)
cbd.Write(fd, "aaaaaaaa"*512, 0, 4096)
cbd.Write(fd, "bbbbbbbb"*512, 4096, 4096)
  
# Read file
# parameter: the four parameters are
#      fd
#      null string
#      offset
#      data length to read
# return: return read data if read successfully, otherwise return an error code

# The read content is returned by the return value, buf is not needed, you can pass in an empty string
cbd.Read(fd，"", 0, 4096)

# Close file
cbd.Close(fd)
```

Note: The current python api does not support asynchronous reading and writing

#### Delete file

```python
# parameter: the two parameters are
#      filename
#      user information
# return: return 0 if delete successfully, otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"

# Delete file
cbd.Unlink("/curve", user)
```
#### Recover file

```python
# parameter: the three parameters are
#      filename
#      user information
#      file id (optional, default is 0)
# return: return 0 if recover successfully, otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"

# Recover file
cbd.Recover("/curve", user, 0)
```

#### Rename file

```python
# parameter: the three parameters are
#      user information
#      old filename
#      new filename
# return: return 0 if rename successfully,  otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"

# Rename
cbd.Rename(user, "/curve", "/curve-new")
```

#### Create a directory

```python
# parameter: the two parameters are
#      directory path
#      user information
# return: return 0 if create successfully,  otherwise return an error code

# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"
# Create directory
cbd.Mkdir("/curvedir", user)
```

#### Delete directory

```python
# parameter: the two parameters are
#      directory path
#      user information
# return: return 0 if create successfully,  otherwise return an error code
  
# Construct user information
user = curvefs.UserInfo_t()
user.owner = "curve"

# Delete directory
cbd.Rmdir("/curvedir", user)
```

#### List the files in the directory

```python
# parameter: the two parameters are
#      directory path
#      user information
# return: files in the directory (only filename)
files = cbd.Listdir("/test", user)
for f in files:
    print f
```

#### Get the cluster ID

```python
# Determine whether this get is successful by the return value
# success: return cluster ID
# fail: return null string
clusterId = cbd.GetClusterId()
print clusterId
# c355675a-f4d2-4729-b80a-5a7bcc749d1c
```

#### Clean CBDClient object

```python
cbd.UnInit()
```

### Error code

| Code | Message                     | description                     |
| :--: | :-------------------------- | ------------------------ |
|  0   | OK                          | success                 |
|  -1  | EXISTS                      | file or directory exists       |
|  -2  | FAILED                      | fail                 |
|  -3  | DISABLEDIO                  | disable io                   |
|  -4  | AUTHFAIL                    | authentication failed                 |
|  -5  | DELETING                    | deleting                 |
|  -6  | NOTEXIST                    | file not exist               |
|  -7  | UNDER_SNAPSHOT              | under snapshot                   |
|  -8  | NOT_UNDERSNAPSHOT           | not under snapshot               |
|  -9  | DELETE_ERROR                | delete error                 |
| -10  | NOT_ALLOCATE                | segment not allocated           |
| -11  | NOT_SUPPORT                 | operation not supported               |
| -12  | NOT_EMPTY                   | directory not empty                 |
| -13  | NO_SHRINK_BIGGER_FILE       | no shrinkage                 |
| -14  | SESSION_NOTEXISTS           | session not exist           |
| -15  | FILE_OCCUPIED               | file occupied               |
| -16  | PARAM_ERROR                 | parameter error                 |
| -17  | INTERNAL_ERROR              | internal error                |
| -18  | CRC_ERROR                   | CRC error              |
| -19  | INVALID_REQUEST             | parameter invalid         |
| -20  | DISK_FAIL                   | disk fail                 |
| -21  | NO_SPACE                    | no space                 |
| -22  | NOT_ALIGNED                 | io not aligned                 |
| -23  | BAD_FD                      | file is being closed, fd unavailable |
| -24  | LENGTH_NOT_SUPPORT          | file length is not supported       |
| -25  | SESSION_NOT_EXIST           | session not exist, duplicate with -14 |
| -26  | STATUS_NOT_MATCH            | status error                 |
| -27  | DELETE_BEING_CLONED         | delete the file being cloned       |
| -28  | CLIENT_NOT_SUPPORT_SNAPSHOT | this version of client not support snapshot   |
| -29  | SNAPSHOT_FROZEN             | snapshot is disabled       |
| -100 | UNKNOWN                     | unknown error                 |
