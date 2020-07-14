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

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "nbd/src/texttable.h"

namespace curve {
namespace nbd {

using std::string;

void TextTable::define_column(const string &heading,
                              enum TextTable::Align hd_align,
                              enum TextTable::Align col_align) {
    TextTableColumn def(heading, heading.length(), hd_align, col_align);
    col.push_back(def);
}

void TextTable::clear() {
    currow = 0;
    curcol = 0;
    indent = 0;
    row.clear();
    // reset widths to heading widths
    for (unsigned int i = 0; i < col.size(); i++)
        col[i].width = col[i].heading.size();
}

/**
 * Pad s with space to appropriate alignment
 *
 * @param s string to pad
 * @param width width of field to contain padded string
 * @param align desired alignment (LEFT, CENTER, RIGHT)
 *
 * @return padded string
 */
static string
pad(string s, int width, TextTable::Align align) {
    int lpad, rpad;
    lpad = 0;
    rpad = 0;
    switch (align) {
        case TextTable::LEFT:
        rpad = width - s.length();
        break;
        case TextTable::CENTER:
        lpad = width / 2 - s.length() / 2;
        rpad = width - lpad - s.length();
        break;
        case TextTable::RIGHT:
        lpad = width - s.length();
        break;
    }

    return string(lpad, ' ') + s + string(rpad, ' ');
}

std::ostream &operator<<(std::ostream &out, const TextTable &t) {
    for (unsigned int i = 0; i < t.col.size(); i++) {
        TextTable::TextTableColumn col = t.col[i];
        out << string(t.indent, ' ')
            << pad(col.heading, col.width, col.hd_align)
        << ' ';
    }
    out << std::endl;

    for (unsigned int i = 0; i < t.row.size(); i++) {
        for (unsigned int j = 0; j < t.row[i].size(); j++) {
        TextTable::TextTableColumn col = t.col[j];
        out << string(t.indent, ' ')
        << pad(t.row[i][j], col.width, col.col_align)
        << ' ';
        }
        out << std::endl;
    }
    return out;
}


}  // namespace nbd
}  // namespace curve
