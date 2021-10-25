/*
 *     Copyright (c) 2020 NetEase Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/**
 * Project: curve
 * Date: Thu Jul  8 15:36:28 CST 2021
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "nbd/src/define.h"

namespace curve {
namespace nbd {

TEST(NBDConfigTest, TestMapOptions) {
    ASSERT_EQ("defaults", NBDConfig{}.MapOptions());

    {
        NBDConfig config;
        config.try_netlink = true;
        config.readonly = true;

        ASSERT_EQ("read-only,try-netlink", config.MapOptions());
    }

    {
        NBDConfig config;
        config.try_netlink = true;

        ASSERT_EQ("try-netlink", config.MapOptions());
    }

    {
        NBDConfig config;
        config.try_netlink = true;
        config.timeout = 3600;  // default value

        ASSERT_EQ("try-netlink", config.MapOptions());
    }

    {
        NBDConfig config;
        config.try_netlink = true;
        config.timeout = 3600;  // default value
        config.nebd_conf = "/etc/nebd/nebd-client.conf";

        ASSERT_EQ("try-netlink,nebd-conf=/etc/nebd/nebd-client.conf",
                  config.MapOptions());
    }
}

}  // namespace nbd
}  // namespace curve
