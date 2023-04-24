const Sha256 = require("@aws-crypto/sha256-js");
const SignatureV4 = require("@aws-sdk/signature-v4");
const HttpRequest = require("@aws-sdk/protocol-http");
const SnappyJS = require("snappyjs");
const protobuf = require("protobufjs");
const prom = require("./prom");
const axios =  require("axios");

function initSigner(awsAuth) {
  return new SignatureV4.SignatureV4({
    service: 'aps',
    region: 'us-east-1',
    sha256: Sha256.Sha256,
    credentials: awsAuth,
  });

}

const __holder = {
  type: null,
};

const kv = (o) =>
  typeof o === "object"
    ? Object.entries(o).map((e) => ({
        name: e[0],
        value: e[1],
      }))
    : undefined;

/** Loads protocol definition, cache it */
async function loadProto(options) {
  if (__holder.root) {
    return __holder.type;
  }

  if (options?.proto) {
    const root = await protobuf.load(options?.proto);
    if (options?.verbose) {
      console.info("Loaded protocol definitions", options?.proto, root);
    }
    const WriteRequest = root.lookupType("prometheus.WriteRequest");
    __holder.type = WriteRequest;
    return WriteRequest;
  }

  return prom.prometheus.WriteRequest;
}

/** Serializes JSON as protobuf buffer */
async function serialize(payload, options) {
  const type = await loadProto(options);
  const errMsg = type.verify(payload);
  if (errMsg) {
    throw new Error(errMsg);
  }
  const buffer = type.encode(payload).finish();
  return buffer;
}

/**
 * Sends metrics over HTTP(s)
 *
 * @param {import("./types").Timeseries | import("./types").Timeseries[]} timeseries
 * @param {import("./types").Options} options
 * @return {Promise<import("./types").Result>}
 */
async function pushTimeseries(timeseries, options) {

  // Brush up a little
  timeseries = !Array.isArray(timeseries) ? [timeseries] : timeseries;

  // Nothing to do
  if (timeseries.length === 0) {
    return {
      status: 200,
      statusText: "OK",
    };
  }

  const start1 = Date.now();
  const writeRequest = {
    timeseries: timeseries.map((t) => ({
      labels: Array.isArray(t.labels)
        ? [t.labels, ...(kv(options?.labels) || [])]
        : kv({
            ...options?.labels,
            ...t.labels,
          }),
      samples: t.samples.map((s) => ({
        value: s.value,
        timestamp: s.timestamp ? s.timestamp : Date.now(),
      })),
    })),
  };
  const buffer = await serialize(writeRequest, options?.proto);

  const logger = options?.console || console;

  const start2 = Date.now();
  if (options?.timing) {
    logger.info("Serialized in", start2 - start1, "ms");
  }
  const signer = initSigner(options?.awsAuth)
  const awsManagedPrometheusHostname = options?.hostname;
  const pathPrefix = options?.url;
  const awsManagedPrometheusRemoteWrite = `${pathPrefix}/remote_write`;
  const request = new HttpRequest.HttpRequest({
      method: 'POST',
      protocol: 'https:',
      path: awsManagedPrometheusRemoteWrite,
      body: SnappyJS.compress(buffer),
      headers: {
          host: awsManagedPrometheusHostname,
          "Content-Type": "application/vnd.google.protobuf",
      },
      hostname: awsManagedPrometheusHostname,
  });
  const signedRequest = await signer.sign(request);
  const url = `https://${signedRequest.hostname}${signedRequest.path}`;
  const response = await axios.default.post(url, signedRequest.body, { headers: signedRequest.headers});
  if (response.status != 200) {
    logger.warn("Failed to send write request, error", response.status + " " + response.statusText, writeRequest);
  }

      return {
        status: response.status,
        statusText: response.statusText,
        errorMessage: response.status !== 200 ? response.statusText : undefined,
      };
  }

async function pushMetrics(metrics, options) {
  return pushTimeseries(
    Object.entries(metrics).map((c) => ({
      labels: { __name__: c[0] },
      samples: [{ value: c[1] }],
    })),
    options
  );
}

module.exports = {
  serialize,
  loadProto,
  pushTimeseries,
  pushMetrics,
};
