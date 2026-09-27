"use strict";

const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const protobuf = require("protobufjs");

const PROTO_ROOT = path.join(__dirname, "..", "proto");

const PROTO_FILES = [
  "app/proxyman/command/command.proto",
  "app/stats/command/command.proto",
];

const PBJS_PROTO_FILES = [
  ...PROTO_FILES,
  "proxy/vless/account.proto",
  "common/serial/typed_message.proto",
];

const packageDefinition = protoLoader.loadSync(PROTO_FILES, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [PROTO_ROOT],
});

const proto = grpc.loadPackageDefinition(packageDefinition);

let pbRoot = null;
async function getPbRoot() {
  if (pbRoot) return pbRoot;

  const root = new protobuf.Root();
  root.resolvePath = (origin, target) => {
    if (path.isAbsolute(target)) return target;
    return path.join(PROTO_ROOT, target);
  };

  pbRoot = await root.load(PBJS_PROTO_FILES.map((f) => path.join(PROTO_ROOT, f)));
  return pbRoot;
}

class XrayGrpcClient {
  constructor(address) {
    this.address = address;
    this.handler = new proto.xray.app.proxyman.command.HandlerService(
      address,
      grpc.credentials.createInsecure()
    );
    this.stats = new proto.xray.app.stats.command.StatsService(
      address,
      grpc.credentials.createInsecure()
    );
  }

  _call(client, method, request, timeoutMs = 4000) {
    return new Promise((resolve, reject) => {
      const deadline = new Date(Date.now() + timeoutMs);
      client[method](request, { deadline }, (err, res) => {
        if (err) return reject(err);
        resolve(res);
      });
    });
  }

  async _packTypedMessage(typeName, payload) {
    const root = await getPbRoot();
    const MsgType = root.lookupType(typeName);
    const errMsg = MsgType.verify(payload);
    if (errMsg) throw new Error(`protobuf verify failed: ${errMsg}`);
    const message = MsgType.create(payload);
    const bytes = MsgType.encode(message).finish();
    return {
      type: typeName,
      value: Buffer.from(bytes),
    };
  }

  /**
   * Добавляет VLESS-пользователя. flow ЖЁСТКО ЗАФИКСИРОВАН пустым: flow
   * "xtls-rprx-vision" работает только поверх raw TCP+XTLS — с транспортом
   * ws (наша новая схема TLS+WS за Cloudflare) он невалиден и Xray его
   * просто проигнорирует/отклонит. Раньше здесь был параметр flow, завязанный
   * на Reality — с уходом от Reality он больше не нужен.
   */
  async addVlessUser({ tag, email, uuid, flow = "" }) {
    const account = await this._packTypedMessage("xray.proxy.vless.Account", {
      id: uuid,
      flow,
      encryption: "none",
    });

    const user = { level: 0, email, account };

    const operation = await this._packTypedMessage(
      "xray.app.proxyman.command.AddUserOperation",
      { user }
    );

    return this._call(this.handler, "alterInbound", { tag, operation });
  }

  async removeUser({ tag, email }) {
    const operation = await this._packTypedMessage(
      "xray.app.proxyman.command.RemoveUserOperation",
      { email }
    );
    return this._call(this.handler, "alterInbound", { tag, operation });
  }

  async getUserStats(email) {
    const [up, down] = await Promise.allSettled([
      this._call(this.stats, "getStats", {
        name: `user>>>${email}>>>traffic>>>uplink`,
        reset: false,
      }),
      this._call(this.stats, "getStats", {
        name: `user>>>${email}>>>traffic>>>downlink`,
        reset: false,
      }),
    ]);

    // Отсутствующий счётчик означает нулевой расход. Ошибка API/таймаут
    // не означает сброс: иначе следующий опрос повторно начислит трафик.
    const failure = [up, down].find((r) => r.status === 'rejected' && r.reason.code !== grpc.status.NOT_FOUND);
    if (failure) throw failure.reason;

    const value = (res) =>
      res.status === "fulfilled" && res.value && res.value.stat
        ? Number(res.value.stat.value)
        : 0;

    return {
      bytes_uploaded: value(up),
      bytes_downloaded: value(down),
    };
  }

  async ping() {
    try {
      await this._call(this.stats, "getStats", {
        name: "__health_probe__",
        reset: false,
      });
      return true;
    } catch (err) {
      if (err && err.code === grpc.status.NOT_FOUND) return true;
      return false;
    }
  }
}

module.exports = { XrayGrpcClient };
