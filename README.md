
-----------------

运行展示

<img src="logo/sql.png" alt="BusTub SQL" width="400">



## Build

推荐在 Ubuntu 24.04 系统或 macOS 系统（M1/M2/Intel 芯片）上使用。

### Linux (Recommended) / macOS (Development Only)

为确保你的机器上安装了所需软件包，请运行以下脚本自动安装：

```console
# Linux
$ sudo build_support/packages.sh
# macOS
$ build_support/packages.sh
```

随后执行以下命令构建系统：

```console
$ mkdir build
$ cd build
$ cmake ..
$ make
```

如果你想以调试模式编译系统，请向 cmake 传入以下参数：
Debug mode:

```console
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make -j`nproc`
```
该模式会默认启用 AddressSanitizer。

若你想使用其他类型的 sanitizer，可执行：

```console
$ cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER=thread ..
$ make -j`nproc`
```

