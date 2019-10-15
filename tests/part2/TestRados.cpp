/*
 ** Project: nebd
 ** File Created: 2019-09-30
 ** Author: hzwuhongsong
 ** Copyright (c) 2019 NetEase
 **/

#include <cmock.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <string>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/typeof/typeof.hpp>
#include "src/part2/rados_interface.h"
#include "tests/part2/test_rados.h"

TEST(OpenImage, OpenImage_1) {
    rados_t cluster;
    const char* poolname = "rbd";
    const char* volname = "volume01";
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    NebdServerMocker mock;
    EXPECT_CALL(mock,
                rados_ioctx_create(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(-1, OpenImage(&cluster, poolname, volname, fd, filename));
}

TEST(OpenImage, OpenImage_2) {
    rados_t cluster;
    const char* poolname = "rbd";
    const char* volname = "volume01";
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    NebdServerMocker mock;
    EXPECT_CALL(mock,
                rados_ioctx_create(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(
        mock, rbd_open(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, rados_ioctx_destroy(::testing::_))
        .WillOnce(testing::Return());
    EXPECT_EQ(-1, OpenImage(&cluster, poolname, volname, fd, filename));
}

TEST(OpenImage, OpenImage_3) {
    rados_t cluster;
    const char* poolname = "rbd";
    const char* volname = "volume01";
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    NebdServerMocker mock;
    EXPECT_CALL(mock,
                rados_ioctx_create(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(
        mock, rbd_open(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(4, OpenImage(&cluster, poolname, volname, fd, filename));
}

TEST(CloseImage, CloseImage_1) {
    g_imageMap.clear();
    EXPECT_EQ(-1, CloseImage(4));
}

TEST(CloseImage, CloseImage_2) {
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_ioctx_destroy(::testing::_))
        .WillOnce(testing::Return());
    EXPECT_CALL(mock, CloseRados(::testing::_)).WillOnce(testing::Return());
    EXPECT_CALL(mock, rbd_close(::testing::_)).WillOnce(testing::Return(0));

    EXPECT_EQ(0, CloseImage(4));
}

TEST(ConnectRados, ConnectRados_1) {
    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_create(::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(NULL, ConnectRados("test"));
}

TEST(ConnectRados, ConnectRados_2) {
    ::system("rm /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_create(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_EQ(NULL, ConnectRados("test"));
}

TEST(ConnectRados, ConnectRados_3) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_create(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_read_file(::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, rados_conf_set(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, CloseRados(::testing::_)).WillOnce(testing::Return());
    EXPECT_EQ(NULL, ConnectRados("test"));
}

TEST(ConnectRados, ConnectRados_4) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_create(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_read_file(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_set(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_CALL(mock, CloseRados(::testing::_)).WillOnce(testing::Return());
    EXPECT_EQ(NULL, ConnectRados("test"));
}

TEST(ConnectRados, ConnectRados_5) {
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_create(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_read_file(::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_set(::testing::_, ::testing::_, ::testing::_))
        .WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseRados(::testing::_)).WillOnce(testing::Return());
    EXPECT_CALL(mock, rados_connect(::testing::_))
        .WillOnce(testing::Return(-1));
    EXPECT_EQ(NULL, ConnectRados("test"));
}
/*
TEST(ConnectRados, ConnectRados_6)
{
    ::system("mkdir -p /etc/nebd");
    ::system("touch /etc/nebd/nebd-server.conf");
    NebdServerMocker mock;
    EXPECT_CALL(mock, rados_create(::testing::_,
::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_read_file(::testing::_,
::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_set(::testing::_, ::testing::_,
::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, CloseRados(::testing::_)).WillOnce(testing::Return());
    EXPECT_CALL(mock, rados_connect(::testing::_)).WillOnce(testing::Return(0));
    EXPECT_EQ(NULL, ConnectRados("test"));
}
*/
/*
TEST(open_image, open_image_1)
{
    NebdServerMocker mock;
    rados_t* cluster = new rados_t;
    const char* poolname = "rbd";
    const char* volname = "volume01";
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    filename[2] = '1';

   EXPECT_CALL(mock, rados_ioctx_create(::testing::_, ::testing::_,
::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_EQ(-1, open_image(cluster, poolname, volname, fd, filename));
}
*/
/*
TEST(ConnectRados, ConnectRados_1)
{
    NebdServerMocker mock;
    rados_t* cluster = new rados_t;
    const char* poolname = "rbd";
    const char* volname = "volume01";
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    filename[2] = '1';

    rados_t* rados = NULL;
    EXPECT_CALL(mock, rados_create(::testing::_,
::testing::_)).WillOnce(testing::Return(-1));
    const char* mon_host = "ttt";
    EXPECT_EQ(rados, ConnectRados(mon_host));
}

TEST(ConnectRados, ConnectRados_2)
{
    NebdServerMocker mock;
    rados_t* cluster = new rados_t;
    const char* poolname = "rbd";
    const char* volname = "volume01";
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    filename[2] = '1';

    rados_t* rados = NULL;
    //EXPECT_CALL(mock, rados_create(::testing::_,
::testing::_)).WillOnce(testing::Return(0));
    EXPECT_CALL(mock, rados_conf_read_file(::testing::_,
::testing::_)).WillOnce(testing::Return(-1));
    const char* mon_host = "ttt";
    EXPECT_EQ(rados, ConnectRados(mon_host));
}
*/
TEST(FilenameFdExist, FilenameFdExist_1) {
    NebdServerMocker mock;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    filename[1] = '1';

    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.clear();
    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    const char* filename_input = "11";
    EXPECT_EQ(4, FilenameFdExist(const_cast<char*>(filename_input)));
}

TEST(FilenameFdExist, FilenameFdExist_2) {
    NebdServerMocker mock;

    g_imageMap.clear();
    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';

    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    const char* filename_input = "11";
    EXPECT_EQ(0, FilenameFdExist(const_cast<char*>(filename_input)));
}

TEST(FdExist, FdExist_1) {
    NebdServerMocker mock;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    filename[2] = '1';

    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    rbd_image_t* image_ret;
    // EXPECT_CALL(mock, rados_ioctx_create(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_TRUE(FdExist(4, &image_ret));
}

TEST(FdExist, FdExist_2) {
    NebdServerMocker mock;

    rados_ioctx_t* io = new rados_ioctx_t;
    rados_t* cluster = new rados_t;
    int fd = 4;
    char* filename = new char[5]();
    filename[0] = '1';
    filename[2] = '1';

    rbd_image_t* image = new rbd_image_t;
    FdImage_t* fd_image = new FdImage_t;
    fd_image->cluster = cluster;
    fd_image->io = io;
    fd_image->image = image;
    fd_image->filename = filename;

    g_imageMap.insert(std::pair<int, FdImage_t*>(fd, fd_image));

    rbd_image_t* image_ret;
    // EXPECT_CALL(mock, rados_ioctx_create(::testing::_, ::testing::_,
    // ::testing::_)).WillOnce(testing::Return(-1));

    EXPECT_FALSE(FdExist(8, &image_ret));
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
