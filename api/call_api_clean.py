import os
import logging
from datetime import datetime

import grpc
from google.protobuf import empty_pb2
import challenger_pb2 as ch
import challenger_pb2_grpc as api

op = [('grpc.max_send_message_length', 10 * 1024 * 1024),
      ('grpc.max_receive_message_length', 100 * 1024 * 1024)]
with grpc.insecure_channel('challenge2024.debs.org:5023', options=op) as channel:
    stub = api.ChallengerStub(channel)

    # Step 1 - Crear un nuevo Benchmark
    benchmarkconfiguration = ch.BenchmarkConfiguration(
        token='ljhcowvpnamgmyxfnsrqvhimyvcjdhzz',
        benchmark_name="this name shows_up_in_dashboard",
        benchmark_type="test",
        queries=[ch.Query.Q1, ch.Query.Q2]
    )
    benchmark = stub.createNewBenchmark(benchmarkconfiguration)

    stub.startBenchmark(benchmark)

    cnt_current = 0
    cnt_historic = 0
    cnt = 0

    batch = stub.nextBatch(benchmark)
    while batch:
        for drive_state in batch.states:
            print("Drive State:")
            print("Timestamp:", drive_state.date)
            print("Serial Number:", drive_state.serial_number)
            print("Model:", drive_state.model)
            print("Failure:", drive_state.failure)
            print("Vault ID:", drive_state.vault_id)
            print("Readings:", drive_state.readings)
            print()

        # result_payload_q1 = processTheBatchQ1(batch) #here is your implementation ;)
        resultQ1 = ch.ResultQ1(
            benchmark_id=benchmark.id,
            batch_seq_id=batch.seq_id,
            topkimproved=[]
        )
        stub.resultQ1(resultQ1)

        if batch.last or cnt > 1_000:
            break

        cnt = cnt + 1
        batch = stub.nextBatch(benchmark)

    stub.endMeasurement(benchmark)

print("Finished")
