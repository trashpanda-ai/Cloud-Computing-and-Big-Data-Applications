import os
import logging
from datetime import datetime

# Si falta grpc: pip install grpcio
import grpc
from google.protobuf import empty_pb2

# Si faltan las clases, genéralas:
# Necesitas instalar grpcio-tools para generar los stubs: pip install grpcio-tools
# python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. challenger.proto
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

    # Step 2 - Obtener todas las ubicaciones
    # loc = stub.getLocations(benchmark)
    # print("Fetched %s locations" % (len(loc.locations)))

    # # Step 3 (opcional) - Calibrar la latencia
    # ping = stub.initializeLatencyMeasuring(benchmark)
    # for i in range(10):
    #     ping = stub.measure(ping)
    # stub.endMeasurement(ping)

    # Step 4 - Iniciar el procesamiento de eventos y comenzar el reloj
    stub.startBenchmark(benchmark)
    # ¡El reloj está en marcha!

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

        # cnt_current += len(batch.states)
        # cnt_historic += len(batch.lastyear)

        # if(cnt % 100) == 0:
        #     ts_str = ""
        #     if len(batch.states) > 0:  # Podría ocurrir que solo haya eventos del año pasado disponibles
        #         ts = batch.states[0].date
        #         dt = datetime.utcfromtimestamp(ts.seconds)
        #         ts_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        #     print("Processed %s - current_time: %s, num_current: %s, num_historic: %s, total_events: %s" %
        #           (cnt, ts_str, cnt_current, cnt_historic, (cnt_current + cnt_historic)))


        # result_payload_q1 = processTheBatchQ1(batch) #here is your implementation ;)
        resultQ1 = ch.ResultQ1(
            benchmark_id=benchmark.id,
            batch_seq_id=batch.seq_id,
            topkimproved=[]
        )
        stub.resultQ1(resultQ1)

        #processTheBatchQ1(batch) # here should be the implementation of Q2
        resultQ2 = ch.ResultQ2(
            benchmark_id=benchmark.id,
            batch_seq_id=batch.seq_id,
            histogram=[]
        )
        stub.resultQ2(resultQ2)

        if batch.last or cnt > 1_000:
            break

        cnt = cnt + 1
        batch = stub.nextBatch(benchmark)

    stub.endMeasurement(benchmark)

print("Finished")
