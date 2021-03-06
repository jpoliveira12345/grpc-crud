package br.com.bomnegocio.service;

import br.com.bomnegocio.dto.House;
import br.com.bomnegocio.dto.HouseServiceGrpc;
import com.google.protobuf.Empty;
import com.google.protobuf.Int64Value;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.lognet.springboot.grpc.GRpcService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@GRpcService
@SuppressWarnings("unused")
public class Server extends HouseServiceGrpc.HouseServiceImplBase {

    // Database
    Map<Int64Value, House> houses = new HashMap<>();

    // Exception
    private static final String EXCEPTION_MESSAGE = "House not found!!!";

    @Override
    public void getHouse(Int64Value id, StreamObserver<House> responseObserver) {
        var house = houses.get(id);
        if( Objects.isNull(house) ) {
            RuntimeException ex = Status.NOT_FOUND
                    .withDescription(EXCEPTION_MESSAGE)
                    .asRuntimeException();
            responseObserver.onError(ex);
        }

        responseObserver.onNext(house);
        responseObserver.onCompleted();
    }

    @Override
    public void createHouse(House house, StreamObserver<House> responseObserver) {

        houses.put(house.getId(), house);
        responseObserver.onNext(house);
        responseObserver.onCompleted();
    }

    @Override
    public void changeHouse(House house, StreamObserver<House> responseObserver) {
        houses.put(house.getId(), house);
        responseObserver.onNext(house);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteHouse(Int64Value id, StreamObserver<House> responseObserver) {
        var house = houses.remove(id);
        if( Objects.isNull(house) ) {
            RuntimeException ex = Status.NOT_FOUND
                    .withDescription(EXCEPTION_MESSAGE)
                    .asRuntimeException();
            responseObserver.onError(ex);
        }

        responseObserver.onNext(house);
        responseObserver.onCompleted();
    }

    @Override
    public void findAll(Empty request, StreamObserver<House> responseObserver) {
        var housesList = new ArrayList<>(houses.values());

        if(housesList.isEmpty()){
            RuntimeException ex = Status.UNAVAILABLE
                    .withDescription("There is no house registered!!!")
                    .asRuntimeException();
            responseObserver.onError(ex);
        }

        for( var h : housesList){
            responseObserver.onNext(h);
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<Int64Value> findFirst(StreamObserver<House> responseObserver) {
        return new StreamObserver<>() {
            @Override
            //Quando o cliente envia dados
            public void onNext(Int64Value id) {
                var housesList = new ArrayList<>(houses.values());
                for( var house : housesList){
                    if(house.getId().equals(id)){
                        responseObserver.onNext(house);
                        responseObserver.onCompleted();
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            // Quando o cliente faz um commit
            public void onCompleted() {
                RuntimeException ex = Status.NOT_FOUND
                        .withDescription(EXCEPTION_MESSAGE)
                        .asRuntimeException();
                responseObserver.onError(ex);
            }
        };
    }

    @Override
    public StreamObserver<House> createAll(StreamObserver<House> responseObserver) {
        return new StreamObserver<>() {
            @Override
            //Quando o cliente envia dados
            public void onNext(House house) {
                houses.put(house.getId(), house);
                responseObserver.onNext(house);
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            // Quando o cliente faz um commit
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
