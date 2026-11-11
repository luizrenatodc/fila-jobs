# fila-jobs

Fila de processamento em segundo plano para Node.js, **sem dependências**: o tipo de peça que fica entre o seu sistema e APIs instáveis (emissão de nota, envio de e-mail, webhooks).

## O que ela garante

- **Prioridade**: jobs urgentes passam na frente, com ordem de chegada mantida no empate.
- **Concorrência limitada**: nunca mais que N jobs ao mesmo tempo (não derruba a API do parceiro).
- **Novas tentativas com backoff exponencial + jitter**: espaça as tentativas e evita que todos tentem no mesmo instante.
- **Timeout por job**: job travado conta como falha.
- **Erro permanente**: dado inválido vai direto para a fila de mortos, sem gastar tentativas.
- **Idempotência**: a mesma `chave` (ex.: `pedido-42`) nunca gera dois jobs.
- **Fila de mortos (DLQ)** com `reprocessar(id)` depois de corrigir a causa.

## Uso

```js
import { Fila, ErroPermanente } from "./src/fila.js";

const fila = new Fila({ concorrencia: 3, maxTentativas: 4 });
fila.registrar("enviar-email", async ({ para }) => {
  if (!para.includes("@")) throw new ErroPermanente("e-mail inválido");
  await enviar(para);
});

fila.adicionar("enviar-email", { para: "cliente@exemplo.com" }, { chave: "boas-vindas-17", prioridade: 5 });
await fila.ocioso(); // { pendente: 0, executando: 0, concluido: 1, falhou: 0, morto: 0 }
```

## Rodar

```bash
npm test     # 11 testes
npm run demo # simula emissão de notas numa API com 30% de falha
```

## Decisões

- **Equal jitter** (`teto/2 + aleatório·teto/2`): nunca espera zero e nunca passa do teto.
- **Agendador injetável**: dá para trocar `setTimeout` por um relógio falso nos testes.
- **Estados explícitos** (`pendente → executando → concluido | falhou → morto`) e evento `aoMudar` para painel ou métricas.
- Em memória de propósito: a interface é a mesma que eu usaria sobre Redis ou Postgres (`SELECT ... FOR UPDATE SKIP LOCKED`).

Licença MIT.
