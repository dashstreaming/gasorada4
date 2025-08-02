"""
Servicio de base de datos - OPTIMIZADO para velocidad
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from statistics import mean

from sqlalchemy import select, and_, or_, func, desc, text
from sqlalchemy.orm import selectinload

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
                              offset: int = 0) -> List[Dict]:
        """
        OPTIMIZADA: Obtiene gasolineras usando SQL directo para mayor velocidad
        """
        async with async_session() as session:
            
            # Query base optimizada con JOIN para obtener precios en una sola consulta
            base_query = """
                SELECT DISTINCT
                    gs.id,
                    gs.name,
                    gs.brand,
                    gs.address,
                    gs.city,
                    gs.state,
                    gs.latitude,
                    gs.longitude,
                    gs.has_magna,
                    gs.has_premium,
                    gs.has_diesel,
                    gs.average_rating,
                    gs.total_reviews
                FROM gas_stations gs
                WHERE gs.is_active = true
            """
            
            params = {}
            conditions = []
            
            # Filtro por ubicación (optimizado con bounding box)
            if latitude and longitude and radius_km:
                # Usar bounding box en lugar de cálculos complejos de distancia
                lat_delta = radius_km / 111.0  # ~111 km por grado
                lng_delta = radius_km / (111.0 * abs(latitude / 90.0))
                
                conditions.append("""
                    gs.latitude BETWEEN :min_lat AND :max_lat
                    AND gs.longitude BETWEEN :min_lng AND :max_lng
                """)
                params.update({
                    'min_lat': latitude - lat_delta,
                    'max_lat': latitude + lat_delta,
                    'min_lng': longitude - lng_delta,
                    'max_lng': longitude + lng_delta
                })
            
            # Filtros de texto
            if city:
                conditions.append("gs.city ILIKE :city")
                params['city'] = f"%{city}%"
                
            if state:
                conditions.append("gs.state ILIKE :state")
                params['state'] = f"%{state}%"
                
            if brand:
                conditions.append("gs.brand ILIKE :brand")
                params['brand'] = f"%{brand}%"
            
            # Filtro por tipo de combustible
            if fuel_type:
                fuel_type = fuel_type.lower()
                if fuel_type == "magna":
                    conditions.append("gs.has_magna = true")
                elif fuel_type == "premium":
                    conditions.append("gs.has_premium = true")
                elif fuel_type == "diesel":
                    conditions.append("gs.has_diesel = true")
            
            # Agregar condiciones al query
            if conditions:
                base_query += " AND " + " AND ".join(conditions)
            
            # Ordenamiento y límite
            if latitude and longitude:
                # Ordenamiento aproximado por distancia
                base_query += """
                    ORDER BY (ABS(gs.latitude - :user_lat) + ABS(gs.longitude - :user_lng))
                """
                params.update({'user_lat': latitude, 'user_lng': longitude})
            else:
                base_query += " ORDER BY gs.name"
            
            base_query += " LIMIT :limit OFFSET :offset"
            params.update({'limit': limit, 'offset': offset})
            
            # Ejecutar query principal
            result = await session.execute(text(base_query), params)
            stations_data = result.fetchall()
            
            if not stations_data:
                return []
            
            # Obtener IDs de las estaciones
            station_ids = [str(row[0]) for row in stations_data]
            
            # Query optimizada para precios actuales
            prices_query = """
                SELECT 
                    gp.gas_station_id,
                    gp.fuel_type,
                    gp.price,
                    gp.source,
                    gp.confidence_score,
                    gp.created_at
                FROM gas_prices gp
                WHERE gp.gas_station_id = ANY(:station_ids)
                AND gp.is_current = true
                AND gp.validation_status = 'validated'
                ORDER BY gp.created_at DESC
            """
            
            prices_result = await session.execute(
                text(prices_query), 
                {'station_ids': station_ids}
            )
            
            # Organizar precios por estación
            prices_by_station = {}
            for price_row in prices_result.fetchall():
                station_id, fuel_type, price, source, confidence, created_at = price_row
                
                if station_id not in prices_by_station:
                    prices_by_station[station_id] = {}
                
                if fuel_type not in prices_by_station[station_id]:
                    prices_by_station[station_id][fuel_type] = {
                        'price': float(price),
                        'source': source,
                        'confidence': float(confidence),
                        'updated_at': created_at.isoformat(),
                        'age_hours': (datetime.utcnow() - created_at).total_seconds() / 3600,
                        'is_fresh': (datetime.utcnow() - created_at).total_seconds() < 86400
                    }
            
            # Construir resultado final
            result_stations = []
            for row in stations_data:
                station_id = str(row[0])
                
                station_dict = {
                    'id': station_id,
                    'name': row[1],
                    'brand': row[2],
                    'address': row[3],
                    'city': row[4],
                    'state': row[5],
                    'latitude': float(row[6]) if row[6] else None,
                    'longitude': float(row[7]) if row[7] else None,
                    'services': {
                        'magna': bool(row[8]),
                        'premium': bool(row[9]),
                        'diesel': bool(row[10])
                    },
                    'stats': {
                        'average_rating': float(row[11]) if row[11] else 0.0,
                        'total_reviews': int(row[12]) if row[12] else 0
                    },
                    'current_prices': prices_by_station.get(station_id, {})
                }
                
                # Calcular distancia si hay coordenadas
                if latitude and longitude and station_dict['latitude'] and station_dict['longitude']:
                    station_dict['distance_km'] = self._calculate_distance(
                        latitude, longitude,
                        station_dict['latitude'], station_dict['longitude']
                    )
                
                result_stations.append(station_dict)
            
            return result_stations
    
    def _calculate_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Cálculo rápido de distancia usando fórmula haversine simplificada"""
        from math import sin, cos, sqrt, atan2, radians
        
        R = 6371.0  # Radio de la Tierra en km
        lat1, lng1, lat2, lng2 = map(radians, [lat1, lng1, lat2, lng2])
        
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlng/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return round(R * c, 2)
    
    async def get_gas_station_by_id(self, station_id: str) -> Optional[Dict]:
        """OPTIMIZADA: Obtiene una gasolinera por ID usando SQL directo"""
        async with async_session() as session:
            query = """
                SELECT 
                    gs.id, gs.name, gs.brand, gs.address, gs.city, gs.state,
                    gs.latitude, gs.longitude, gs.has_magna, gs.has_premium, 
                    gs.has_diesel, gs.average_rating, gs.total_reviews,
                    gs.phone, gs.website
                FROM gas_stations gs
                WHERE gs.id = :station_id AND gs.is_active = true
            """
            
            result = await session.execute(text(query), {'station_id': station_id})
            station_row = result.first()
            
            if not station_row:
                return None
            
            # Obtener precios actuales
            prices_query = """
                SELECT fuel_type, price, source, confidence_score, created_at
                FROM gas_prices
                WHERE gas_station_id = :station_id 
                AND is_current = true 
                AND validation_status = 'validated'
            """
            
            prices_result = await session.execute(
                text(prices_query), 
                {'station_id': station_id}
            )
            
            current_prices = {}
            for fuel_type, price, source, confidence, created_at in prices_result.fetchall():
                current_prices[fuel_type] = {
                    'price': float(price),
                    'source': source,
                    'confidence': float(confidence),
                    'updated_at': created_at.isoformat(),
                    'age_hours': (datetime.utcnow() - created_at).total_seconds() / 3600
                }
            
            return {
                'id': str(station_row[0]),
                'name': station_row[1],
                'brand': station_row[2],
                'address': station_row[3],
                'city': station_row[4],
                'state': station_row[5],
                'latitude': float(station_row[6]) if station_row[6] else None,
                'longitude': float(station_row[7]) if station_row[7] else None,
                'services': {
                    'magna': bool(station_row[8]),
                    'premium': bool(station_row[9]),
                    'diesel': bool(station_row[10])
                },
                'stats': {
                    'average_rating': float(station_row[11]) if station_row[11] else 0.0,
                    'total_reviews': int(station_row[12]) if station_row[12] else 0
                },
                'contact': {
                    'phone': station_row[13],
                    'website': station_row[14]
                },
                'current_prices': current_prices
            }
    
    async def get_current_prices_all_stations(self, 
                                            fuel_type: Optional[str] = None,
                                            city: Optional[str] = None,
                                            state: Optional[str] = None,
                                            limit: int = 100) -> List[Dict]:
        """OPTIMIZADA: Obtiene precios actuales usando SQL directo"""
        async with async_session() as session:
            query = """
                SELECT 
                    gs.id as gas_station_id,
                    gs.name as gas_station_name,
                    gs.address as gas_station_address,
                    gs.brand as gas_station_brand,
                    gs.city, gs.state, gs.latitude, gs.longitude,
                    gp.fuel_type, gp.price, gp.source, 
                    gp.confidence_score, gp.created_at
                FROM gas_stations gs
                JOIN gas_prices gp ON gs.id = gp.gas_station_id
                WHERE gs.is_active = true 
                AND gp.is_current = true 
                AND gp.validation_status = 'validated'
            """
            
            params = {}
            conditions = []
            
            if fuel_type:
                conditions.append("gp.fuel_type = :fuel_type")
                params['fuel_type'] = fuel_type.lower()
            
            if city:
                conditions.append("gs.city ILIKE :city")
                params['city'] = f"%{city}%"
            
            if state:
                conditions.append("gs.state ILIKE :state")
                params['state'] = f"%{state}%"
            
            if conditions:
                query += " AND " + " AND ".join(conditions)
            
            query += " ORDER BY gp.price ASC LIMIT :limit"
            params['limit'] = limit
            
            result = await session.execute(text(query), params)
            
            prices_data = []
            for row in result.fetchall():
                age_hours = (datetime.utcnow() - row[11]).total_seconds() / 3600
                
                prices_data.append({
                    "gas_station_id": str(row[0]),
                    "gas_station_name": row[1],
                    "gas_station_address": row[2],
                    "gas_station_brand": row[3],
                    "fuel_type": row[8],
                    "price": float(row[9]),
                    "source": row[10],
                    "confidence": float(row[11]) if row[11] else 1.0,
                    "updated_at": row[12].isoformat() if row[12] else None,
                    "age_hours": age_hours,
                    "location": {
                        "latitude": float(row[6]) if row[6] else None,
                        "longitude": float(row[7]) if row[7] else None,
                        "city": row[4],
                        "state": row[5]
                    }
                })
            
            return prices_data
    
    # Mantener los métodos originales para create_price_report, create_review, etc.
    # (copiar del archivo original ya que no necesitan optimización)
    
    async def create_price_report(self, report_data: dict, request_ip: str) -> UserPriceReport:
        """Crea un nuevo reporte de precio (sin cambios)"""
        async with async_session() as session:
            report = UserPriceReport.create_from_form_data(
                report_data, 
                {"ip": request_ip}
            )
            
            session.add(report)
            await session.flush()
            
            price = GasPrice.create_from_user_report(
                gas_station_id=report.gas_station_id,
                fuel_type=report.fuel_type,
                price=report.reported_price,
                reporter_ip=request_ip,
                notes=report.comments,
                pump_number=report.pump_number
            )
            
            session.add(price)
            report.process_report()
            
            await session.commit()
            await session.refresh(report)
            
            logger.info(f"Created price report: {report.id}")
            return report
    
    async def get_price_statistics(self, fuel_type: str, region: Optional[str] = None) -> Dict:
        """OPTIMIZADA: Estadísticas de precios usando SQL directo"""
        async with async_session() as session:
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            
            query = """
                SELECT gp.price
                FROM gas_prices gp
                JOIN gas_stations gs ON gp.gas_station_id = gs.id
                WHERE gp.fuel_type = :fuel_type
                AND gp.created_at >= :cutoff_date
                AND gp.is_current = true
                AND gp.validation_status = 'validated'
            """
            
            params = {'fuel_type': fuel_type.lower(), 'cutoff_date': cutoff_date}
            
            if region:
                query += " AND (gs.state ILIKE :region OR gs.city ILIKE :region)"
                params['region'] = f"%{region}%"
            
            result = await session.execute(text(query), params)
            prices = [float(row[0]) for row in result.fetchall()]
            
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


# Instancia global del servicio
db_service = DatabaseService()