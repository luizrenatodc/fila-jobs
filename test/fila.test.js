import { test } from "node:test";
import assert from "node:assert/strict";
import { ErroPermanente, Fila, atrasoBackoff } from "../src/fila.js";

const rapido = { backoff: { baseMs: 1, maxMs: 5 }, timeoutMs: 200 };

test("executa jobs e guarda o resultado", async () => {
  const f = new Fila(rapido).registrar("soma", ({ a, b }) => a + b);
  const j = f.adicionar("soma", { a: 2, b: 3 });
  await f.ocioso();
  assert.equal(j.estado, "concluido");
  assert.equal(j.resultado, 5);
});

test("respeita o limite de concorrência", async () => {
  let ativos = 0, pico = 0;
  const f = new Fila({ ...rapido, concorrencia: 2 }).registrar("lento", async () => {
    pico = Math.max(pico, ++ativos);
    await new Promise((r) => setTimeout(r, 10));
    ativos--;
  });
  for (let i = 0; i < 6; i++) f.adicionar("lento", {});
  await f.ocioso();
  assert.equal(pico, 2);
});

test("maior prioridade sai primeiro", async () => {
  const ordem = [];
  const f = new Fila({ ...rapido, concorrencia: 1 }).registrar("t", (d) => { ordem.push(d); });
  f.adicionar("t", "baixa", { prioridade: 0 });
  f.adicionar("t", "alta", { prioridade: 9 });
  f.adicionar("t", "media", { prioridade: 5 });
  await f.ocioso();
  assert.deepEqual(ordem, ["alta", "media", "baixa"]);
});

test("tenta de novo e conclui quando a falha é passageira", async () => {
  let n = 0;
  const f = new Fila(rapido).registrar("instavel", () => { if (++n < 3) throw new Error("rede"); return "ok"; });
  const j = f.adicionar("instavel", {});
  await f.ocioso();
  assert.equal(j.estado, "concluido");
  assert.equal(j.tentativas, 3);
  assert.deepEqual(j.erros, ["rede", "rede"]);
});

test("esgota as tentativas e vai para a fila de mortos", async () => {
  const f = new Fila({ ...rapido, maxTentativas: 2 }).registrar("quebra", () => { throw new Error("sempre"); });
  const j = f.adicionar("quebra", {});
  await f.ocioso();
  assert.equal(j.estado, "morto");
  assert.equal(f.mortos().length, 1);
});

test("erro permanente não gasta novas tentativas", async () => {
  const f = new Fila(rapido).registrar("valida", () => { throw new ErroPermanente("CPF inválido"); });
  const j = f.adicionar("valida", {});
  await f.ocioso();
  assert.equal(j.estado, "morto");
  assert.equal(j.tentativas, 1);
});

test("timeout conta como falha", async () => {
  const f = new Fila({ ...rapido, timeoutMs: 20, maxTentativas: 1 }).registrar("trava", () => new Promise(() => {}));
  const j = f.adicionar("trava", {});
  await f.ocioso();
  assert.equal(j.estado, "morto");
  assert.deepEqual(j.erros, ["timeout"]);
});

test("chave de idempotência evita job duplicado", async () => {
  let execucoes = 0;
  const f = new Fila(rapido).registrar("email", () => { execucoes++; });
  const a = f.adicionar("email", { para: "x" }, { chave: "pedido-42" });
  const b = f.adicionar("email", { para: "x" }, { chave: "pedido-42" });
  await f.ocioso();
  assert.equal(a, b);
  assert.equal(execucoes, 1);
});

test("job morto pode ser reprocessado", async () => {
  let quebrado = true;
  const f = new Fila({ ...rapido, maxTentativas: 1 }).registrar("t", () => { if (quebrado) throw new Error("x"); return 1; });
  const j = f.adicionar("t", {});
  await f.ocioso();
  quebrado = false;
  f.reprocessar(j.id);
  await f.ocioso();
  assert.equal(j.estado, "concluido");
});

test("backoff cresce, tem jitter e respeita o teto", () => {
  const b = (t, r) => atrasoBackoff(t, { baseMs: 100, maxMs: 1000, aleatorio: () => r });
  assert.equal(b(1, 0), 50);
  assert.equal(b(3, 1), 400);
  assert.equal(b(10, 1), 1000);
  assert.ok(b(10, 0) >= 500);
});

test("tipo sem handler é rejeitado", () => {
  assert.throws(() => new Fila().adicionar("nada", {}), /sem handler/);
});
