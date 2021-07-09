/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.string;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@UdfDescription(name = "to_bytes",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    category = FunctionCategory.STRING,
    description = "blablabla.")
public class ToBytes {

  @Udf(description = "blahhh")
  public ByteBuffer toBytes(
      @UdfParameter(
          description = "The source string. If null, then function returns null.") final String str,
      @UdfParameter(
          description = "The encoding of the string.") final String encoding) {
    if (str == null || encoding == null) {
      return null;
    }

    switch (encoding.toLowerCase()) {
      case "hex":
        try {
          return ByteBuffer.wrap(Hex.decodeHex(str));
        } catch (DecoderException e) {
          throw new KsqlFunctionException("hex fail");
        }
      case "base64":
        return ByteBuffer.wrap(Base64.getMimeDecoder().decode(str));
      case "utf8":
        return ByteBuffer.wrap(str.getBytes(UTF_8));
      case "ascii":
        return ByteBuffer.wrap(str.getBytes(US_ASCII));
      default:
        throw new KsqlFunctionException("Supported encodings are: hex, utf8, ascii and base64");
    }
  }
}
