from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
import asyncio
import json
import aiohttp
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import AsyncFunction

OLLAMA_URL = "http://ollama:11434/api/generate"
PREDICOES_VALIDAS = {1, 2}


def montar_prompt(perfil_json: str) -> str:
    return f"""Você é um analista de risco de crédito bancário experiente.

Ao avaliar um perfil, considere apenas estes fatores, nesta ordem de importância:
1. status_conta_corrente: saldo negativo ou ausência de conta é sinal de risco.
2. historico_credito: atrasos passados ou "conta crítica" são sinais de risco.
3. valor_credito_solicitado em relação a duracao_credito_meses: valores altos
com prazos longos aumentam o risco de inadimplência.
4. proposito_emprestimo: propósitos produtivos tendem a ser menos arriscados
que consumo quando combinados com outros sinais de risco.

Nunca compare o valor solicitado com preços de produtos ou serviços não
relacionados ao perfil. Baseie-se exclusivamente nos campos fornecidos.

Exemplo 1:
Perfil: {{"status_conta_corrente": ">= 200 DM (Saldo Alto)", "duracao_credito_meses": 12, "historico_credito": "Créditos pagos neste banco", "proposito_emprestimo": "Educação", "valor_credito_solicitado": 1500}}
Resposta: {{"predicao": 1, "justificativa": "Saldo alto e histórico de créditos pagos neste banco indicam baixo risco."}}

Exemplo 2:
Perfil: {{"status_conta_corrente": "< 0 DM (Saldo Negativo)", "duracao_credito_meses": 48, "historico_credito": "Atrasos passados", "proposito_emprestimo": "Carro novo", "valor_credito_solicitado": 9500}}
Resposta: {{"predicao": 2, "justificativa": "Saldo negativo, atrasos passados e valor alto em prazo longo indicam alto risco."}}

Agora avalie o perfil abaixo. Responda APENAS em JSON com os campos "predicao" (1 ou 2) e "justificativa":
Perfil: {perfil_json}"""


class AvaliarCreditoAsync(AsyncFunction):
    """Chama o Ollama de forma assíncrona — não bloqueia a task do Flink
    esperando resposta, permitindo múltiplas chamadas em voo simultaneamente
    dentro do mesmo slot."""

    def open(self, runtime_context):
        self.session = None

    async def _get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session

    async def async_invoke(self, value: str):
        cliente = json.loads(value)
        target_real = cliente.get("target_real", -1)
        id_cliente = cliente.get("id_cliente", -1)

        perfil = {k: v for k, v in cliente.items() if k != "target_real"}
        perfil_json = json.dumps(perfil, ensure_ascii=False)

        try:
            session = await self._get_session()
            async with session.post(
                OLLAMA_URL,
                json={
                    "model": "llama3.2:1b-instruct-fp16",
                    "prompt": montar_prompt(perfil_json),
                    "format": "json",
                    "stream": False,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                resultado = json.loads(data["response"])
                predicao = resultado.get("predicao")
                justificativa = str(resultado.get("justificativa", ""))

                if predicao not in PREDICOES_VALIDAS:
                    predicao = 0
                    justificativa = (
                        f"predicao_invalida_recebida={resultado.get('predicao')}"
                    )

        except Exception as e:
            predicao = 0
            justificativa = f"erro: {e}"

        saida = {
            "id_cliente": id_cliente,
            "target_real": target_real,
            "predicao": predicao,
            "justificativa": justificativa,
        }
        return [json.dumps(saida, ensure_ascii=False)]

    def close(self):
        if self.session is not None:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            if loop.is_running():
                # loop já ativo (comum durante shutdown do Flink) —
                # agenda o fechamento sem bloquear
                asyncio.ensure_future(self.session.close())
            else:
                loop.run_until_complete(self.session.close())


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("broker:29092")
        .set_topics("credit_applications_raw")
        .set_group_id("flink-llm-scoring")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("broker:29092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("credit_applications_scored")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    from pyflink.datastream import AsyncDataStream

    scored = AsyncDataStream.unordered_wait(
        ds,
        AvaliarCreditoAsync(),
        timeout=Time.seconds(60),
        capacity=4,
        output_type=Types.STRING(),
    )

    scored.sink_to(sink)
    env.execute("finstream-llm-scoring-async")


if __name__ == "__main__":
    main()
