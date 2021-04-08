package br.com.bomnegocio;

import br.com.bomnegocio.dto.House;
import br.com.bomnegocio.dto.HouseServiceGrpc;
import br.com.bomnegocio.dto.HouseType;
import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static br.com.bomnegocio.dto.HouseServiceGrpc.newBlockingStub;

@Slf4j
public class ClientTest {

    @Test
    public void createHouse(){
        log.info("Client starting...");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 10000).usePlaintext().build();

        HouseServiceGrpc.HouseServiceBlockingStub serverStub = newBlockingStub(channel);

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

        House response = serverStub.createHouse(house);

        log.info(response.toString());
        log.info("Shutting down channel");
        channel.shutdown();
    }

    @Test
    public void getHouse(){
        log.info("Client starting...");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 10000).usePlaintext().build();

        HouseServiceGrpc.HouseServiceBlockingStub serverStub = newBlockingStub(channel);

        House response = null;
        try {
            response = serverStub.getHouse(Int64Value.of(1L));
        } catch (StatusRuntimeException ex){
            Status status = ex.getStatus();
            log.info(status.getDescription());
        }

        if (response != null) {
            log.info(response.toString());
        }
        log.info("Shutting down channel");
        channel.shutdown();
    }

    @Test
    public void findAll(){
        log.info("Client starting...");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 10000).usePlaintext().build();

        HouseServiceGrpc.HouseServiceBlockingStub serverStub = newBlockingStub(channel);

        List<House> response = new ArrayList<>();
        serverStub.findAll(Empty.getDefaultInstance()).forEachRemaining(response::add);

        log.info(response.toString());
        log.info("Shutting down channel");
        channel.shutdown();
    }

    @Test
    public void changeHouse(){
        log.info("Client starting...");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 10000).usePlaintext().build();

        HouseServiceGrpc.HouseServiceBlockingStub serverStub = newBlockingStub(channel);

        House house = serverStub.getHouse(Int64Value.of(1L));
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

        House response = serverStub.changeHouse(newHouse);
        log.info(response.toString());
        log.info("Shutting down channel");
        channel.shutdown();
    }

    @Test
    public void deleteHouse(){
        log.info("Client starting...");

        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 10000).usePlaintext().build();

        HouseServiceGrpc.HouseServiceBlockingStub serverStub = newBlockingStub(channel);

        var id = Int64Value.of(1L);
        House response = serverStub.deleteHouse(id);
        log.info(response.toString());
        log.info("Shutting down channel");
        channel.shutdown();
    }
}
