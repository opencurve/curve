/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-08-01
 * Author: NetEase Media Bigdata
 */

package io.opencurve.curve.fs.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.DelegateToFileSystem;
import io.opencurve.curve.fs.flink.CurveFileSystemFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The CurveFS implementation of AbstractFileSystem.
 * This impl delegates to the old FileSystem
 */
public class CurveFS extends DelegateToFileSystem {
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   *
   * @param theUri which must be that of localFs
   * @param conf
   * @throws IOException
   * @throws URISyntaxException
   */
  CurveFS(final URI theUri, final Configuration conf) throws IOException,
    URISyntaxException {
    super(theUri, new CurveFileSystem(conf), conf, CurveFileSystemFactory.SCHEME, true);
  }
}
