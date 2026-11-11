// Fila de jobs em memória com: prioridade, limite de concorrência, novas tentativas com
// backoff exponencial + jitter, timeout por job, chave de idempotência e fila de mortos (DLQ).
// O relógio e o agendador são injetáveis para testes determinísticos.

export const ESTADOS = ["pendente", "executando", "concluido", "falhou", "morto"];

export function atrasoBackoff(tentativa, { baseMs = 200, maxMs = 30_000, aleatorio = Math.random } = {}) {
  const teto = Math.min(maxMs, baseMs * 2 ** (tentativa - 1));
  return Math.round(teto / 2 + aleatorio() * (teto / 2)); // "equal jitter": nunca zero, nunca acima do teto
}

export class Fila {
  #handlers = new Map();
  #jobs = new Map();
  #prontos = [];
  #idem = new Map();
  #ativos = 0;
  #seq = 0;
  #ouvintes = new Set();

  constructor({ concorrencia = 2, maxTentativas = 3, timeoutMs = 10_000, backoff = {}, agendar = setTimeout } = {}) {
    Object.assign(this, { concorrencia, maxTentativas, timeoutMs, backoff, agendar });
  }

  registrar(tipo, handler) { this.#handlers.set(tipo, handler); return this; }
  aoMudar(fn) { this.#ouvintes.add(fn); return () => this.#ouvintes.delete(fn); }

  adicionar(tipo, dados, { prioridade = 0, chave } = {}) {
    if (!this.#handlers.has(tipo)) throw new Error(`tipo sem handler: ${tipo}`);
    if (chave && this.#idem.has(chave)) return this.#jobs.get(this.#idem.get(chave));
    const job = { id: ++this.#seq, tipo, dados, prioridade, tentativas: 0, estado: "pendente", erros: [], resultado: undefined };
    this.#jobs.set(job.id, job);
    if (chave) this.#idem.set(chave, job.id);
    this.#enfileirar(job);
    return job;
  }

  job(id) { return this.#jobs.get(id); }
  mortos() { return [...this.#jobs.values()].filter((j) => j.estado === "morto"); }
  resumo() {
    const r = Object.fromEntries(ESTADOS.map((e) => [e, 0]));
    for (const j of this.#jobs.values()) r[j.estado]++;
    return r;
  }

  /** Reenfileira um job morto (ex.: depois de corrigir a causa). */
  reprocessar(id) {
    const j = this.#jobs.get(id);
    if (!j || j.estado !== "morto") throw new Error("só jobs mortos podem ser reprocessados");
    j.tentativas = 0; j.estado = "pendente";
    this.#enfileirar(j);
  }

  /** Resolve quando não há nada pendente, executando ou aguardando nova tentativa. */
  ocioso() {
    return new Promise((ok) => {
      const checar = () => {
        const r = this.resumo();
        if (!r.pendente && !r.executando && !r.falhou) { parar(); ok(r); }
      };
      const parar = this.aoMudar(checar);
      checar();
    });
  }

  #emitir(job) { for (const fn of this.#ouvintes) fn(job); }

  #enfileirar(job) {
    // inserção estável: maior prioridade primeiro, ordem de chegada no empate
    const i = this.#prontos.findIndex((x) => x.prioridade < job.prioridade);
    i === -1 ? this.#prontos.push(job) : this.#prontos.splice(i, 0, job);
    this.#emitir(job);
    queueMicrotask(() => this.#bombear());
  }

  #bombear() {
    while (this.#ativos < this.concorrencia && this.#prontos.length) this.#executar(this.#prontos.shift());
  }

  async #executar(job) {
    this.#ativos++;
    job.estado = "executando";
    job.tentativas++;
    this.#emitir(job);
    let timer;
    try {
      const limite = new Promise((_, falha) => { timer = this.agendar(() => falha(new Error("timeout")), this.timeoutMs); });
      job.resultado = await Promise.race([this.#handlers.get(job.tipo)(job.dados, job), limite]);
      job.estado = "concluido";
    } catch (e) {
      job.erros.push(String(e?.message ?? e));
      if (e?.permanente || job.tentativas >= this.maxTentativas) {
        job.estado = "morto";
      } else {
        job.estado = "falhou";
        const espera = atrasoBackoff(job.tentativas, this.backoff);
        this.agendar(() => { job.estado = "pendente"; this.#enfileirar(job); }, espera);
      }
    } finally {
      clearTimeout(timer);
      this.#ativos--;
      this.#emitir(job);
      this.#bombear();
    }
  }
}

/** Erro que não adianta tentar de novo (dados inválidos, 4xx): vai direto para a DLQ. */
export class ErroPermanente extends Error {
  permanente = true;
}
