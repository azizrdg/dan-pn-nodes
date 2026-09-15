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

// Отдельный список для protobufjs (_packAny/getPbRoot): помимо файлов,
// которые нужны gRPC-сервисам (HandlerService/StatsService) через
// proto-loader выше, сюда обязательно нужно включать ЛЮБОЙ .proto,
// чьи типы паковаются вручную через _packAny() — в частности
// account.proto с xray.proxy.vless.Account, который нигде не
// импортируется из command.proto/user.proto (там google.protobuf.Any
// стоит как заглушка, реальный тип protobufjs должен знать сам).
// Раньше этого файла тут не было — root.lookupType("xray.proxy.vless.Account")
// падал с "no such type: xray.proxy.vless.Account", даже когда
// ENOENT на common/protocol/user.proto уже был исправлен.
const PBJS_PROTO_FILES = [
  ...PROTO_FILES,
  "proxy/vless/account.proto",
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

// protobufjs используется отдельно, чтобы вручную закодировать
// AddUserOperation/RemoveUserOperation/Account в google.protobuf.Any,
// так как proto-loader не паковает Any сам по себе.
let pbRoot = null;
async function getPbRoot() {
  if (pbRoot) return pbRoot;

  const root = new protobuf.Root();
  // Резолвим все относительные импорты (import "common/protocol/user.proto";)
  // от PROTO_ROOT, а не от директории импортирующего файла — так же,
  // как это делает includeDirs у @grpc/proto-loader выше.
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

  async _packAny(typeName, payload) {
    const root = await getPbRoot();
    const MsgType = root.lookupType(typeName);
    const errMsg = MsgType.verify(payload);
    if (errMsg) throw new Error(`protobuf verify failed: ${errMsg}`);
    const message = MsgType.create(payload);
    const bytes = MsgType.encode(message).finish();
    return {
      type_url: `type.googleapis.com/${typeName}`,
      value: Buffer.from(bytes),
    };
  }

  /**
   * Добавляет VLESS-пользователя в указанный inbound (по tag) через
   * HandlerService.AlterInbound + AddUserOperation.
   */
  async addVlessUser({ tag, email, uuid, flow }) {
    const account = await this._packAny("xray.proxy.vless.Account", {
      id: uuid,
      flow: flow || "xtls-rprx-vision",
      encryption: "none",
    });

    const user = { level: 0, email, account };

    const operation = await this._packAny(
      "xray.app.proxyman.command.AddUserOperation",
      { user }
    );

    return this._call(this.handler, "alterInbound", { tag, operation });
  }

  async removeUser({ tag, email }) {
    const operation = await this._packAny(
      "xray.app.proxyman.command.RemoveUserOperation",
      { email }
    );
    return this._call(this.handler, "alterInbound", { tag, operation });
  }

  /**
   * Возвращает { uplink, downlink } в байтах для email/tag пользователя.
   * Требует statsUserUplink/statsUserDownlink включённых в policy Xray
   * (bootstrap.sh включает stats: {} и policy.system/levels).
   */
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
    // Лёгкий способ проверить доступность api-инбаунда: запросить
    // несуществующую метрику, ошибка "not found" — это тоже "живой" gRPC.
    try {
      await this._call(this.stats, "getStats", {
        name: "__health_probe__",
        reset: false,
      });
      return true;
    } catch (err) {
      // NOT_FOUND (5) означает что gRPC сервер отвечает, просто нет такой метрики
      if (err && err.code === grpc.status.NOT_FOUND) return true;
      return false;
    }
  }
}

module.exports = { XrayGrpcClient };