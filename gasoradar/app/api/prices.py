"""
API endpoints para precios - OPTIMIZADO para mejor performance
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Form

from ..services.db_service import db_service
from ..services.protection_service import protection_service


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/prices", tags=["Prices"])


@router.get("/current")
async def get_current_prices(
    fuel_type: Optional[str] = Query(None, description="Filtrar por tipo de combustible"),
    city: Optional[str] = Query(None, description="Filtrar por ciudad"),
    state: Optional[str] = Query(None, description="Filtrar por estado"),
    latitude: Optional[float] = Query(None, description="Latitud para búsqueda por cercanía"),
    longitude: Optional[float] = Query(None, description="Longitud para búsqueda por cercanía"),
    radius_km: Optional[int] = Query(25, ge=1, le=100, description="Radio de búsqueda en km"),
    sort_by: Optional[str] = Query("price", description="Ordenar por: price, updated, distance"),
    limit: int = Query(50, ge=1, le=200, description="Límite de resultados")
):
    """
    Obtiene precios actuales de combustible con filtros opcionales - OPTIMIZADO
    """
    try:
        logger.info(f"🚀 Obteniendo precios actuales optimizado - fuel_type={fuel_type}, limit={limit}")
        
        # Usar método optimizado
        prices_data = await db_service.get_current_prices_all_stations_optimized(
            fuel_type=fuel_type,
            city=city,
            state=state,
            limit=limit
        )
        
        # Si hay coordenadas, calcular distancias y filtrar por radio
        if latitude and longitude and radius_km:
            filtered_prices = []
            for price_data in prices_data:
                lat = price_data["location"]["latitude"]
                lng = price_data["location"]["longitude"]
                
                # Calcular distancia simple
                from math import sqrt
                lat_diff = lat - latitude
                lng_diff = lng - longitude
                distance_approx = sqrt(lat_diff**2 + lng_diff**2) * 111  # Aproximación en km
                
                if distance_approx <= radius_km:
                    price_data["distance_km"] = round(distance_approx, 2)
                    filtered_prices.append(price_data)
            
            prices_data = filtered_prices
        
        # Ordenamiento
        if sort_by == "updated":
            prices_data.sort(key=lambda x: x["updated_at"], reverse=True)
        elif sort_by == "distance" and latitude and longitude:
            prices_data.sort(key=lambda x: x.get("distance_km", 999))
        else:  # price
            prices_data.sort(key=lambda x: x["price"])
        
        logger.info(f"✅ Precios actuales optimizado completado - {len(prices_data)} precios")
        
        return {
            "prices": prices_data,
            "total": len(prices_data),
            "filters": {
                "fuel_type": fuel_type,
                "city": city,
                "state": state,
                "radius_km": radius_km if latitude and longitude else None
            },
            "performance": {
                "optimized": True,
                "single_query": True
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo precios actuales: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/current/legacy")
async def get_current_prices_legacy(
    fuel_type: Optional[str] = Query(None, description="Filtrar por tipo de combustible"),
    city: Optional[str] = Query(None, description="Filtrar por ciudad"),
    state: Optional[str] = Query(None, description="Filtrar por estado"),
    latitude: Optional[float] = Query(None, description="Latitud para búsqueda por cercanía"),
    longitude: Optional[float] = Query(None, description="Longitud para búsqueda por cercanía"),
    radius_km: Optional[int] = Query(25, ge=1, le=100, description="Radio de búsqueda en km"),
    sort_by: Optional[str] = Query("price", description="Ordenar por: price, updated, distance"),
    limit: int = Query(50, ge=1, le=200, description="Límite de resultados")
):
    """
    VERSIÓN LEGACY (más lenta) - Solo para comparación
    """
    try:
        logger.warning(f"⚠️ Usando método LEGACY para precios actuales - fuel_type={fuel_type}, limit={limit}")
        
        # Usar método legacy (más lento)
        prices_data = await db_service.get_current_prices_all_stations(
            fuel_type=fuel_type,
            city=city,
            state=state,
            limit=limit
        )
        
        # Resto del código igual...
        if latitude and longitude and radius_km:
            filtered_prices = []
            for price_data in prices_data:
                lat = price_data["location"]["latitude"]
                lng = price_data["location"]["longitude"]
                
                from math import sqrt
                lat_diff = lat - latitude
                lng_diff = lng - longitude
                distance_approx = sqrt(lat_diff**2 + lng_diff**2) * 111
                
                if distance_approx <= radius_km:
                    price_data["distance_km"] = round(distance_approx, 2)
                    filtered_prices.append(price_data)
            
            prices_data = filtered_prices
        
        if sort_by == "updated":
            prices_data.sort(key=lambda x: x["updated_at"], reverse=True)
        elif sort_by == "distance" and latitude and longitude:
            prices_data.sort(key=lambda x: x.get("distance_km", 999))
        else:
            prices_data.sort(key=lambda x: x["price"])
        
        logger.warning(f"⚠️ Precios actuales legacy completado (lento) - {len(prices_data)} precios")
        
        return {
            "prices": prices_data,
            "total": len(prices_data),
            "filters": {
                "fuel_type": fuel_type,
                "city": city,
                "state": state,
                "radius_km": radius_km if latitude and longitude else None
            },
            "performance": {
                "optimized": False,
                "warning": "Este endpoint es más lento"
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo precios actuales (legacy): {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.post("/report")
async def report_price(
    request: Request,
    gas_station_id: str = Form(..., description="ID de la gasolinera"),
    fuel_type: str = Form(..., description="Tipo de combustible"),
    reported_price: float = Form(..., ge=0, description="Precio por litro"),
    comments: Optional[str] = Form(None, description="Comentarios adicionales"),
    pump_number: Optional[int] = Form(None, description="Número de bomba"),
    reporter_name: Optional[str] = Form(None, description="Nombre del reportero"),
    captcha_token: Optional[str] = Form(None, alias="g-recaptcha-response", description="Token reCAPTCHA")
):
    """
    Reporta un nuevo precio de combustible (CON PROTECCIONES)
    """
    try:
        # Obtener IP del cliente
        client_ip = request.client.host if request.client else "127.0.0.1"
        
        # Preparar datos para validación
        form_data = {
            "gas_station_id": gas_station_id,
            "fuel_type": fuel_type,
            "reported_price": reported_price,
            "comments": comments,
            "pump_number": pump_number,
            "reporter_name": reporter_name,
            "g-recaptcha-response": captcha_token
        }
        
        # VALIDACIONES DE PROTECCIÓN
        validation_ok, validation_msg = await protection_service.validate_price_report(
            form_data, client_ip
        )
        
        if not validation_ok:
            logger.warning(f"⚠️ Validación de reporte falló desde {client_ip}: {validation_msg}")
            raise HTTPException(status_code=400, detail=validation_msg)
        
        # Verificar que la gasolinera existe
        station = await db_service.get_gas_station_by_id(gas_station_id)
        if not station:
            raise HTTPException(status_code=404, detail="Gasolinera no encontrada")
        
        # Verificar que la gasolinera vende este combustible
        if not station.has_fuel_type(fuel_type):
            raise HTTPException(
                status_code=400,
                detail=f"Esta gasolinera no vende {fuel_type}"
            )
        
        # Crear el reporte (esto también crea el precio automáticamente)
        report = await db_service.create_price_report(form_data, client_ip)
        
        logger.info(f"✅ Reporte de precio creado: {report.id} desde {client_ip}")
        
        return {
            "success": True,
            "message": "Precio reportado correctamente",
            "report_id": report.id,
            "gas_station_name": station.name,
            "fuel_type": fuel_type.lower(),
            "reported_price": reported_price,
            "status": report.status
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error reportando precio: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/statistics")
async def get_price_statistics(
    fuel_type: str = Query(..., description="Tipo de combustible"),
    region: Optional[str] = Query(None, description="Región específica")
):
    """
    Obtiene estadísticas de precios por combustible y región
    """
    try:
        logger.info(f"🔍 Obteniendo estadísticas de precios para {fuel_type} en {region or 'nacional'}")
        
        stats = await db_service.get_price_statistics(fuel_type, region)
        
        if "error" in stats:
            raise HTTPException(status_code=404, detail=stats["error"])
        
        logger.info(f"✅ Estadísticas de precios obtenidas para {fuel_type}")
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error obteniendo estadísticas de precios: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/cheapest")
async def get_cheapest_prices(
    fuel_type: str = Query(..., description="Tipo de combustible"),
    city: Optional[str] = Query(None, description="Ciudad"),
    state: Optional[str] = Query(None, description="Estado"),
    limit: int = Query(10, ge=1, le=50, description="Número de resultados")
):
    """
    Encuentra los precios más baratos por región - OPTIMIZADO
    """
    try:
        if not city and not state:
            raise HTTPException(
                status_code=400,
                detail="Debe especificar al menos ciudad o estado"
            )
        
        region = city or state
        logger.info(f"🔍 Buscando precios más baratos de {fuel_type} en {region}")
        
        # Usar método optimizado
        stations = await db_service.search_stations_by_region(region, fuel_type, limit)
        
        if not stations:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron gasolineras en {region}"
            )
        
        logger.info(f"✅ Encontrados {len(stations)} precios baratos en {region}")
        
        return {
            "search_type": "region",
            "region": region,
            "fuel_type": fuel_type.lower(),
            "total_stations_found": len(stations),
            "stations": stations,
            "performance": {
                "optimized": True,
                "single_query": True
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error buscando precios más baratos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


@router.get("/validation-info")
async def get_price_validation_info(
    fuel_type: str = Query(..., description="Tipo de combustible"),
    region: Optional[str] = Query(None, description="Región para validación")
):
    """
    Obtiene información sobre los rangos de validación actuales
    """
    try:
        logger.info(f"🔍 Obteniendo info de validación para {fuel_type} en {region or 'nacional'}")
        
        # Usar el servicio de protección para obtener info de validación
        is_valid, message, validation_info = await protection_service.validate_price_dynamically(
            fuel_type, 0, region  # Usar precio 0 solo para obtener rangos
        )
        
        logger.info(f"✅ Info de validación obtenida para {fuel_type}")
        
        return {
            "fuel_type": fuel_type,
            "region": region or "nacional",
            "validation_info": validation_info,
            "message": "Rangos de validación actuales"
        }
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo info de validación: {str(e)}")
        raise HTTPException(status_code=500, detail="Error obteniendo información de validación")


@router.get("/debug/performance-comparison")
async def debug_price_performance():
    """
    Endpoint para comparar performance entre método optimizado y legacy
    """
    try:
        import time
        
        # Test método optimizado
        start_opt = time.time()
        prices_opt = await db_service.get_current_prices_all_stations_optimized(
            fuel_type="magna", 
            limit=20
        )
        time_opt = time.time() - start_opt
        
        # Test método legacy
        start_leg = time.time()
        prices_leg = await db_service.get_current_prices_all_stations(
            fuel_type="magna", 
            limit=20
        )
        time_leg = time.time() - start_leg
        
        return {
            "performance_comparison": {
                "optimized_method": {
                    "time_seconds": round(time_opt, 3),
                    "prices_count": len(prices_opt),
                    "method": "Single JOIN query",
                    "status": "✅ RÁPIDO"
                },
                "legacy_method": {
                    "time_seconds": round(time_leg, 3),
                    "prices_count": len(prices_leg),
                    "method": "Traditional query",
                    "status": "⚠️ MÁS LENTO"
                },
                "improvement_factor": round(time_leg / time_opt, 1) if time_opt > 0 else "∞",
                "recommendation": "Usar método optimizado para mejor performance"
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error en debug de performance de precios: {str(e)}")
        raise HTTPException(status_code=500, detail="Error en debug de performance")