package br.com.bomnegocio;

import br.com.bomnegocio.dto.House;
import br.com.bomnegocio.dto.HouseServiceGrpc;
import br.com.bomnegocio.dto.HouseType;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static br.com.bomnegocio.dto.HouseServiceGrpc.newBlockingStub;
import static br.com.bomnegocio.dto.HouseServiceGrpc.newStub;

@Slf4j
public class ClientTest {

    private ManagedChannel channel;
    private HouseServiceGrpc.HouseServiceBlockingStub syncSercerStub;
    private HouseServiceGrpc.HouseServiceStub asyncServer;

    @Before
    public void init() {
        log.info("Client starting...");
        channel = ManagedChannelBuilder.forAddress("localhost", 10000).usePlaintext().build();
        syncSercerStub = newBlockingStub(channel);
        asyncServer = newStub(channel);
    }

    @After
    public void end() {
        log.info("Shutting down channel");
        channel.shutdown();
    }

    @Test
    public void createHouse() {
        // ID=1
        House house = House.newBuilder()
                .setId(Int64Value.of(1L))
                .addAllResidentNames(Arrays.asList("João", "Paulo"))
                .setAddress("Rua dos bobos, 0")
                .setType(HouseType.CASA)
                .setSize(100)
                .setPrice(10000L)
                .setIsRented(false)
                .build();

        House response = syncSercerStub.createHouse(house);

        print(response);
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @Test
    @SneakyThrows
    public void getHouse() {

        House response = null;
        try {
            response = syncSercerStub.getHouse(Int64Value.of(1L));
        } catch (StatusRuntimeException ex) {
            Status status = ex.getStatus();
            log.info(status.getDescription());
        }

        if (response != null) {
            print(response);
        }
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @Test
    public void findAll() {
        List<House> response = new ArrayList<>();
        syncSercerStub.findAll(Empty.getDefaultInstance()).forEachRemaining(response::add);

        print(response);
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @Test
    public void changeHouse() {
        House house = syncSercerStub.getHouse(Int64Value.of(1L));
        // ID=1
        House newHouse = House.newBuilder()
                .setId(house.getId())
                .addAllResidentNames(house.getResidentNamesList())
                .setAddress(house.getAddress())
                .setType(HouseType.APARTAMENTO)
                .setSize(house.getSize())
                .setPrice(house.getPrice())
                .setIsRented(house.getIsRented())
                .build();

        House response = syncSercerStub.changeHouse(newHouse);
        print(response);
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @Test
    public void deleteHouse() {
        var id = Int64Value.of(1L);
        House response = syncSercerStub.deleteHouse(id);
        print(response);
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @Test
    @SneakyThrows
    public void findFirst() {
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<Int64Value> requestObserver = asyncServer.findFirst(new StreamObserver<>() {
            @Override
            public void onNext(House house) {
                log.info("Received a response from the Server");
                log.info(house.toString());
            }

            @Override
            public void onError(Throwable t) {
                log.info("Error on Service");
            }

            @Override
            public void onCompleted() {
                log.info("Server has completed sending us something");
                latch.countDown();
            }
        });

        requestObserver.onNext(Int64Value.of(-1L));
        requestObserver.onNext(Int64Value.of(-2L));
        requestObserver.onNext(Int64Value.of(3L));

        latch.await(5L, TimeUnit.SECONDS);
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @Test
    @SneakyThrows
    public void createAll() {
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<House> requestObserver = asyncServer.createAll(new StreamObserver<>() {
            @Override
            public void onNext(House house) {
                log.info("Received a response from the Server");
                log.info(house.toString());
            }

            @Override
            public void onError(Throwable t) {
                log.info("Error on Service");
            }

            @Override
            public void onCompleted() {
                log.info("Server has completed sending us something");
                latch.countDown();
            }
        });

        requestObserver.onNext(getCasaJoao());
        requestObserver.onNext(getCasaJorge());
        requestObserver.onNext(getCasaPedro());

        latch.await(5L, TimeUnit.SECONDS);
        log.info("Shutting down channel");
        channel.shutdown();
        Assertions.assertTrue(true);
    }

    @SneakyThrows
    private void print(MessageOrBuilder obj) {
        final var jsonPrinter = JsonFormat.printer();
        final var json = jsonPrinter.print(obj);
        log.info("\n" + json);
    }

    @SneakyThrows
    private <T extends GeneratedMessageV3> void print(List<T> objs) {
        final var jsonPrinter = JsonFormat.printer();
        objs.forEach( x -> {
            try {
                log.info("\n" + jsonPrinter.print(x) + "\n");
            } catch (InvalidProtocolBufferException e) {
                log.info(e.getLocalizedMessage());
            }
        });
    }

    private House getCasaJoao() {
        return House.newBuilder()
                .setId(Int64Value.of(1L))
                .addAllResidentNames(Arrays.asList("João", "Paulo"))
                .setAddress("Rua dos bobos, 0")
                .setType(HouseType.CASA)
                .setSize(100)
                .setPrice(10000L)
                .setIsRented(false)
                .build();
    }

    private House getCasaJorge() {
        return House.newBuilder()
                .setId(Int64Value.of(2L))
                .addAllResidentNames(Arrays.asList("Jorge", "Rondon"))
                .setAddress("Rua dos bobos, 1")
                .setType(HouseType.APARTAMENTO)
                .setSize(200)
                .setPrice(20000L)
                .setIsRented(false)
                .build();
    }

    private House getCasaPedro() {
        return House.newBuilder()
                .setId(Int64Value.of(3L))
                .addAllResidentNames(Arrays.asList("Pedrão", "Silva"))
                .setAddress("Rua dos bobos, 5")
                .setType(HouseType.OUTRO)
                .setSize(200)
                .setPrice(20000L)
                .setIsRented(false)
                .build();
    }

}
