# User System Design

## Proposal

We need a user system to control the access of multiple filesystems and multi
users. It will contains a plugable auth system and some access policies.

## Components and Some Control Flow

### Components

```
MDS:
    FileSystem: basic source unit
    UserSystem: store and CRUD user infos
        - we have the one and only **admin** type user, uid 0
        - admin has full access to all fs and user
        - admin can not own a fs
        - only admin can crud normal users
    AuthSystem: plugable, store and CRUD user auth infos

Client:
    create/mount fs, get/modify user/auth/fs system data
```

### Create Fs

1. Client ---CreateFsRPC---> MDS
    ```
    // owner need be created before request
    CreateFsRPC {
        FsName required
        Owner  required
        Keyring optional
        AllowAnonymousMount optional // default: false
        ... etc
    }
    ```


2. MDS handle CreateFsRPC (user/auth related part only)
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

### Mount Fs

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

2. MDS handle MountFsRPC (user/auth related part only)

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


## Data Structure (pseudocode)

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