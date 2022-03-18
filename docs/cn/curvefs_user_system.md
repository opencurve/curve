# 用户系统设计

## 目标

我们需要一个用户系统来控制在多fs多user场景下的权限。
用户系统会包含一个可替换的认证系统和一些访问规则限制。

## 涉及到的组件和一些控制流

### 组件

```
MDS:
    FileSystem: 基本的资源控制单位
    UserSystem: 负责存储和增删改查用户信息
        - 我们拥有一个唯一的admin，uid分配为0
        - admin拥有对所有fs和普通用户的所有权限
        - admin不拥有fs，fs的owner不能为admin
        - 只有admin有权限增删改查用户基本和认证信息
    AuthSystem: 可插拔替换，负责存储和增删改查用户认证信息

Client:
    增删改查用户/认证信息/文件系统，挂载文件系统
```

### 创建文件系统

1. Client ---CreateFsRPC---> MDS
    ```
    // 用户需要在使用前被创建
    CreateFsRPC {
        FsName required
        Owner  required
        Keyring optional
        AllowAnonymousMount optional // default: false
        ... etc
    }
    ```

2. MDS处理CreateFsRPC (user/auth related part only)
    ```
    if request.OwnerName() == "anonymous":
        // regard as a request from old version
        // do not check auth
    else if request.OwnerName() == "admin":
        // not allowed
    else:
        owner = request.OwnerName()
    
    AuthSystem->do_auth(owner, keyring)
    
    CreateFs
    ```

### 挂载文件系统

1. Client ---MountFsRPC---> MDS
    ```
    MountFsRPC {
        FsName required
        User optional
        Keyring optional
        RequestAccess optional # default: rw
        ... etc
    }
    ```

2. MDS处理MountFsRPC (user/auth related part only)

    ```
    if request.hasUser() == false:
        // request from old version
        if fs.owner == "anonymous":
            do_mount
    else:
        if Fs.AllowAnonymousMount:
            do_mount
        else
            AuthSystem->do_auth(owner, keyring)
            AccessPolicyCheck
                - am i admin?
                - am i owner?
                - am i other user have correct access to this fs?
            do_mount
    ```


## 数据结构（伪代码）

```
proto FileSystem {
    ...
    bool AllowAnonymousMount
    User Owner
    ...
}

proto User {
    UID & Name & Description
    AuthSystemType = {none, simple, etc...}
    // access policy
    OwnFs [fs0, fs1, ...]  assume that owner has full access
    AuthFs {
        fs2: 2 access bit, read/write, 1 for allow, 0 for forbidden
        fs3: ...,
        ...
    }
}

class UserSystem {
    HashMap<UID, User> users
    Method:
        LoadUsers (load users from persistent storage to memory)
        Init (will create an admin)
        ResetAdmin (reset admin's auth info)
        CreateUser (admin only)
        DeleteUser (admin only)
        UpdateUser (admin only)
        GetUser (admin or self)
}


class SimpleAuthSystem {
    HashMap<UID, keyring> auth;
    Method:
        GetAuth
        SetAuth
        DeleteAuth
        AuthCheck
}

```
