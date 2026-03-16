package com.order_service.mapper;


import com.order_service.dto.response.OrderResponse;
import com.order_service.entity.OrderEntity;
import org.mapstruct.Mapper;


@Mapper(componentModel = "spring")
public interface OrderMapper {
    OrderResponse toResponse(OrderEntity entity);
}
