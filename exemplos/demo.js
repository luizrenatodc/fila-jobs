// Simula o envio de notas fiscais para uma API instável (30% de falha) e mostra a fila trabalhando.
import { ErroPermanente, Fila } from "../src/fila.js";

const fila = new Fila({ concorrencia: 3, maxTentativas: 4, backoff: { baseMs: 50, maxMs: 400 } });

fila.registrar("emitir-nota", async ({ pedido, valor }) => {
  await new Promise((r) => setTimeout(r, 30 + Math.random() * 60));
  if (valor <= 0) throw new ErroPermanente("valor inválido");
  if (Math.random() < 0.3) throw new Error("API da prefeitura fora do ar");
  return `NF-${pedido}`;
});

fila.aoMudar((j) => console.log(`#${String(j.id).padStart(2)} ${j.estado.padEnd(10)} tentativa ${j.tentativas}`));

for (let i = 1; i <= 10; i++) fila.adicionar("emitir-nota", { pedido: 1000 + i, valor: i === 7 ? 0 : i * 50 }, { chave: `pedido-${1000 + i}`, prioridade: i % 3 });

console.log("\nResumo:", await fila.ocioso());
for (const m of fila.mortos()) console.log(`DLQ #${m.id}: ${m.erros.at(-1)}`);
