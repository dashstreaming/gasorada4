"""
Servicio de base de datos - OPTIMIZADO para eliminar problema N+1
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from statistics import mean

from sqlalchemy import select, and_, or_, func, desc, text
from sqlalchemy.orm import selectinload, joinedload

from ..database import async_session
from ..models.gas_station import GasStation
from ..models.gas_price import GasPrice
from ..models.user_report import UserPriceReport
from ..models.review import GasStationReview


logger = logging.getLogger(__name__)


class DatabaseService:
    """Servicio optimizado para operaciones de base de datos"""
    
    async def get_gas_stations(self, 
                              latitude: Optional[float] = None,
                              longitude: Optional[float] = None,
                              radius_km: Optional[int] = 25,
                              city: Optional[str] = None,
                              state: Optional[str] = None,
                              brand: Optional[str] = None,
                              fuel_type: Optional[str] = None,
                              limit: int = 50,
                              offset: int = 0) -> List[GasStation]:
        """
        Obtiene gasolineras con filtros opcionales - OPTIMIZADO
        Usa una sola query con JOINs para cargar precios actuales
        """
        async with async_session() as session:
            logger.info(f"🔍 get_gas_stations llamado con lat={latitude}, lng={longitude}, limit={limit}")
            start_time = datetime.utcnow()
            
            # QUERY OPTIMIZADA: Cargar gasolineras con precios en una sola consulta
            query = select(GasStation).where(GasStation.is_active == True)
            
            # Usar selectinload para cargar precios actuales eagerly
            query = query.options(
                selectinload(GasStation.prices).where(
                    and_(
                        GasPrice.is_current == True,
                        GasPrice.validation_status == "validated"
                    )
                )
            )
            
            # Filtros de ubicación - MEJORADOS
            if latitude and longitude and radius_km:
                # Usar la fórmula de Haversine simplificada directamente en SQL
                # Esto es más eficiente que calcular en Python
                lat_rad = func.radians(latitude)
                station_lat_rad = func.radians(GasStation.latitude)
                station_lng_rad = func.radians(GasStation.longitude)
                lng_rad = func.radians(longitude)
                
                # Fórmula de Haversine en SQL
                distance_km = (
                    6371 * func.acos(
                        func.cos(lat_rad) * 
                        func.cos(station_lat_rad) * 
                        func.cos(station_lng_rad - lng_rad) + 
                        func.sin(lat_rad) * 
                        func.sin(station_lat_rad)
                    )
                )
                
                query = query.where(distance_km <= radius_km)
                
                # Ordenar por distancia
                query = query.order_by(distance_km)
            else:
                # Sin coordenadas, ordenar por nombre
                query = query.order_by(GasStation.name)
            
            # Filtros de texto
            if city:
                query = query.where(GasStation.city.ilike(f"%{city}%"))
            if state:
                query = query.where(GasStation.state.ilike(f"%{state}%"))
            if brand:
                query = query.where(GasStation.brand.ilike(f"%{brand}%"))
            
            # Filtro por tipo de combustible
            if fuel_type:
                fuel_type = fuel_type.lower()
                if fuel_type == "magna":
                    query = query.where(GasStation.has_magna == True)
                elif fuel_type == "premium":
                    query = query.where(GasStation.has_premium == True)
                elif fuel_type == "diesel":
                    query = query.where(GasStation.has_diesel == True)
            
            # Paginación
            query = query.offset(offset).limit(limit)
            
            # Ejecutar query
            result = await session.execute(query)
            stations = result.scalars().unique().all()  # unique() para evitar duplicados del join
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"✅ get_gas_stations completado en {elapsed:.3f}s - {len(stations)} estaciones")
            
            return stations
    
    async def get_gas_stations_with_prices_bulk(self, 
                                               station_ids: List[str] = None,
                                               fuel_type: Optional[str] = None,
                                               limit: int = 100) -> List[Dict]:
        """
        Versión optimizada que usa una sola query con JOIN para obtener 
        gasolineras y precios juntos - MUY RÁPIDO
        """
        async with async_session() as session:
            start_time = datetime.utcnow()
            
            # Query con JOIN para obtener todo de una vez
            query = select(
                GasStation.id,
                GasStation.name,
                GasStation.brand,
                GasStation.address,
                GasStation.city,
                GasStation.state,
                GasStation.latitude,
                GasStation.longitude,
                GasStation.has_magna,
                GasStation.has_premium,
                GasStation.has_diesel,
                GasPrice.fuel_type,
                GasPrice.price,
                GasPrice.source,
                GasPrice.confidence_score,
                GasPrice.created_at
            ).select_from(
                GasStation.__table__.join(
                    GasPrice.__table__,
                    and_(
                        GasStation.id == GasPrice.gas_station_id,
                        GasPrice.is_current == True,
                        GasPrice.validation_status == "validated"
                    ),
                    isouter=True  # LEFT JOIN para incluir gasolineras sin precios
                )
            ).where(GasStation.is_active == True)
            
            # Filtros
            if station_ids:
                query = query.where(GasStation.id.in_(station_ids))
            
            if fuel_type:
                query = query.where(
                    or_(
                        GasPrice.fuel_type == fuel_type.lower(),
                        GasPrice.fuel_type.is_(None)  # Incluir gasolineras sin precios
                    )
                )
            
            query = query.order_by(GasStation.name, GasPrice.fuel_type).limit(limit * 3)  # Multiplicar por 3 tipos de combustible
            
            result = await session.execute(query)
            rows = result.all()
            
            # Procesar resultados - agrupar por gasolinera
            stations_dict = {}
            for row in rows:
                station_id = row.id
                
                if station_id not in stations_dict:
                    stations_dict[station_id] = {
                        "id": row.id,
                        "name": row.name,
                        "brand": row.brand,
                        "address": row.address,
                        "city": row.city,
                        "state": row.state,
                        "latitude": row.latitude,
                        "longitude": row.longitude,
                        "services": {
                            "magna": row.has_magna,
                            "premium": row.has_premium,
                            "diesel": row.has_diesel,
                        },
                        "current_prices": {}
                    }
                
                # Agregar precio si existe
                if row.fuel_type and row.price:
                    stations_dict[station_id]["current_prices"][row.fuel_type] = {
                        "price": row.price,
                        "source": row.source,
                        "confidence": row.confidence_score,
                        "updated_at": row.created_at.isoformat(),
                        "age_hours": (datetime.utcnow() - row.created_at).total_seconds() / 3600,
                        "is_fresh": (datetime.utcnow() - row.created_at).total_seconds() / 3600 <= 24
                    }
            
            stations_list = list(stations_dict.values())[:limit]  # Aplicar límite final
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"🚀 get_gas_stations_with_prices_bulk completado en {elapsed:.3f}s - {len(stations_list)} estaciones")
            
            return stations_list
    
    async def get_current_prices_all_stations_optimized(self, 
                                                      fuel_type: Optional[str] = None,
                                                      city: Optional[str] = None,
                                                      state: Optional[str] = None,
                                                      limit: int = 100) -> List[Dict]:
        """
        Versión super optimizada para obtener precios actuales de múltiples gasolineras
        """
        async with async_session() as session:
            start_time = datetime.utcnow()
            
            # Una sola query con JOIN optimizado
            query = select(
                GasPrice.price,
                GasPrice.fuel_type,
                GasPrice.source,
                GasPrice.confidence_score,
                GasPrice.created_at,
                GasStation.id.label('station_id'),
                GasStation.name.label('station_name'),
                GasStation.address,
                GasStation.brand,
                GasStation.latitude,
                GasStation.longitude,
                GasStation.city,
                GasStation.state
            ).select_from(
                GasPrice.__table__.join(GasStation.__table__)
            ).where(
                and_(
                    GasPrice.is_current == True,
                    GasPrice.validation_status == "validated",
                    GasStation.is_active == True
                )
            )
            
            if fuel_type:
                query = query.where(GasPrice.fuel_type == fuel_type.lower())
            
            if city:
                query = query.where(GasStation.city.ilike(f"%{city}%"))
            
            if state:
                query = query.where(GasStation.state.ilike(f"%{state}%"))
            
            query = query.order_by(GasPrice.price.asc()).limit(limit)
            
            result = await session.execute(query)
            rows = result.all()
            
            prices_data = []
            for row in rows:
                age_hours = (datetime.utcnow() - row.created_at).total_seconds() / 3600
                prices_data.append({
                    "gas_station_id": row.station_id,
                    "gas_station_name": row.station_name,
                    "gas_station_address": row.address,
                    "gas_station_brand": row.brand,
                    "fuel_type": row.fuel_type,
                    "price": row.price,
                    "source": row.source,
                    "confidence": row.confidence_score,
                    "updated_at": row.created_at.isoformat(),
                    "age_hours": age_hours,
                    "location": {
                        "latitude": row.latitude,
                        "longitude": row.longitude,
                        "city": row.city,
                        "state": row.state
                    }
                })
            
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"🚀 get_current_prices_all_stations_optimized completado en {elapsed:.3f}s - {len(prices_data)} precios")
            
            return prices_data
    
    async def get_gas_station_by_id(self, station_id: str) -> Optional[GasStation]:
        """Obtiene una gasolinera por ID con precios y reseñas - OPTIMIZADO"""
        async with async_session() as session:
            query = select(GasStation).where(
                and_(
                    GasStation.id == station_id,
                    GasStation.is_active == True
                )
            ).options(
                selectinload(GasStation.prices).where(GasPrice.is_current == True),
                selectinload(GasStation.reviews).where(GasStationReview.status == "approved")
            )
            
            result = await session.execute(query)
            return result.scalar_one_or_none()
    
    async def get_current_prices(self, station_id: str) -> Dict[str, Optional[Dict]]:
        """
        Obtiene los precios actuales de una gasolinera - OPTIMIZADO
        Nota: Esta función solo se usa cuando necesitamos precios de una sola gasolinera
        """
        async with async_session() as session:
            query = select(GasPrice).where(
                and_(
                    GasPrice.gas_station_id == station_id,
                    GasPrice.is_current == True,
                    GasPrice.validation_status == "validated"
                )
            ).order_by(desc(GasPrice.created_at))
            
            result = await session.execute(query)
            prices = result.scalars().all()
            
            # Organizar por tipo de combustible
            current_prices = {}
            for price in prices:
                if price.fuel_type not in current_prices:
                    current_prices[price.fuel_type] = {
                        "price": price.price,
                        "source": price.source,
                        "confidence": price.confidence_score,
                        "updated_at": price.created_at.isoformat(),
                        "age_hours": price.calculate_age_hours(),
                        "is_fresh": price.is_fresh()
                    }
            
            return current_prices
    
    # Resto de métodos sin cambios (create_price_report, create_review, etc.)
    async def create_price_report(self, report_data: dict, request_ip: str) -> UserPriceReport:
        """Crea un nuevo reporte de precio"""
        async with async_session() as session:
            # Crear el reporte
            report = UserPriceReport.create_from_form_data(
                report_data, 
                {"ip": request_ip}
            )
            
            session.add(report)
            await session.flush()  # Para obtener el ID
            
            # Crear precio oficial inmediatamente
            price = GasPrice.create_from_user_report(
                gas_station_id=report.gas_station_id,
                fuel_type=report.fuel_type,
                price=report.reported_price,
                reporter_ip=request_ip,
                notes=report.comments,
                pump_number=report.pump_number
            )
            
            session.add(price)
            
            # Marcar reporte como procesado
            report.process_report()
            
            # Actualizar estadísticas de la gasolinera
            station_query = select(GasStation).where(GasStation.id == report.gas_station_id)
            station_result = await session.execute(station_query)
            station = station_result.scalar_one_or_none()
            
            if station:
                station.total_reports += 1
                station.last_price_update = datetime.utcnow()
            
            await session.commit()
            await session.refresh(report)
            
            logger.info(f"Created price report: {report.id} -> Price: {price.id}")
            return report
    
    async def create_review(self, review_data: dict, request_ip: str) -> GasStationReview:
        """Crea una nueva reseña"""
        async with async_session() as session:
            # Crear la reseña
            review = GasStationReview.create_from_form_data(
                review_data,
                {"ip": request_ip}
            )
            
            session.add(review)
            await session.flush()
            
            # Actualizar estadísticas de la gasolinera
            station_query = select(GasStation).where(GasStation.id == review.gas_station_id)
            station_result = await session.execute(station_query)
            station = station_result.scalar_one_or_none()
            
            if station:
                # Recalcular rating promedio
                if station.total_reviews == 0:
                    station.average_rating = review.rating
                else:
                    total_points = (station.average_rating * station.total_reviews) + review.rating
                    station.average_rating = total_points / (station.total_reviews + 1)
                
                station.total_reviews += 1
            
            await session.commit()
            await session.refresh(review)
            
            logger.info(f"Created review: {review.id}")
            return review
    
    async def get_reviews(self, 
                         station_id: Optional[str] = None,
                         limit: int = 20,
                         offset: int = 0) -> List[GasStationReview]:
        """Obtiene reseñas"""
        async with async_session() as session:
            query = select(GasStationReview).where(
                GasStationReview.status == "approved"
            )
            
            if station_id:
                query = query.where(GasStationReview.gas_station_id == station_id)
            
            query = query.order_by(desc(GasStationReview.created_at))
            query = query.offset(offset).limit(limit)
            
            result = await session.execute(query)
            return result.scalars().all()
    
    async def get_price_statistics(self, fuel_type: str, region: Optional[str] = None) -> Dict:
        """Obtiene estadísticas de precios"""
        async with async_session() as session:
            # Obtener precios recientes
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            
            query = select(GasPrice.price).where(
                and_(
                    GasPrice.fuel_type == fuel_type.lower(),
                    GasPrice.created_at >= cutoff_date,
                    GasPrice.is_current == True,
                    GasPrice.validation_status == "validated"
                )
            )
            
            # Filtrar por región si se especifica
            if region:
                query = query.join(GasStation).where(
                    or_(
                        GasStation.state.ilike(f"%{region}%"),
                        GasStation.city.ilike(f"%{region}%")
                    )
                )
            
            result = await session.execute(query)
            prices = [row[0] for row in result.fetchall()]
            
            if not prices:
                return {"error": "No hay datos de precios disponibles"}
            
            return {
                "fuel_type": fuel_type,
                "region": region or "nacional",
                "sample_size": len(prices),
                "average": round(mean(prices), 2),
                "minimum": min(prices),
                "maximum": max(prices),
                "range": round(max(prices) - min(prices), 2)
            }
    
    async def search_stations_by_region(self, region: str, fuel_type: str, limit: int = 20) -> List[Dict]:
        """Busca estaciones más baratas por región - OPTIMIZADO"""
        async with async_session() as session:
            # Una sola query con JOIN optimizado
            query = select(
                GasStation.id,
                GasStation.name,
                GasStation.brand,
                GasStation.address,
                GasStation.latitude,
                GasStation.longitude,
                GasPrice.price,
                GasPrice.source,
                GasPrice.confidence_score,
                GasPrice.created_at
            ).select_from(
                GasStation.__table__.join(GasPrice.__table__)
            ).where(
                and_(
                    GasStation.is_active == True,
                    or_(
                        GasStation.city.ilike(f"%{region}%"),
                        GasStation.state.ilike(f"%{region}%")
                    ),
                    GasPrice.fuel_type == fuel_type.lower(),
                    GasPrice.is_current == True,
                    GasPrice.validation_status == "validated"
                )
            ).order_by(GasPrice.price.asc()).limit(limit)
            
            result = await session.execute(query)
            rows = result.all()
            
            stations_with_prices = []
            for row in rows:
                age_hours = (datetime.utcnow() - row.created_at).total_seconds() / 3600
                stations_with_prices.append({
                    "gas_station_id": row.id,
                    "name": row.name,
                    "brand": row.brand,
                    "address": row.address,
                    "latitude": row.latitude,
                    "longitude": row.longitude,
                    "price": row.price,
                    "source": row.source,
                    "confidence": row.confidence_score,
                    "updated_at": row.created_at.isoformat(),
                    "age_hours": age_hours
                })
            
            return stations_with_prices


# Instancia global del servicio
db_service = DatabaseService()