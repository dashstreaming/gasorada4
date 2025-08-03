#!/usr/bin/env python3
"""
Script para probar las APIs de Gasoradar
Ejecutar mientras el servidor está corriendo en localhost:8000
"""

import asyncio
import aiohttp
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"

async def test_api_endpoint(session, endpoint, description):
    """Prueba un endpoint específico"""
    url = f"{BASE_URL}{endpoint}"
    print(f"\n🔍 {description}")
    print(f"   URL: {url}")
    
    try:
        async with session.get(url) as response:
            status = response.status
            
            if status == 200:
                try:
                    data = await response.json()
                    print(f"   ✅ Status: {status}")
                    
                    # Mostrar información relevante según el endpoint
                    if 'statistics' in endpoint:
                        print(f"   📊 Datos: {json.dumps(data, indent=2)}")
                    elif 'gas-stations' in endpoint:
                        stations = data.get('stations', [])
                        print(f"   ⛽ Gasolineras encontradas: {len(stations)}")
                        if stations:
                            print(f"   📍 Primera: {stations[0].get('name', 'Sin nombre')} - {stations[0].get('city', 'Sin ciudad')}")
                    elif 'prices' in endpoint:
                        prices = data.get('prices', [])
                        print(f"   💰 Precios encontrados: {len(prices)}")
                        if prices:
                            first_price = prices[0]
                            print(f"   💵 Primer precio: ${first_price.get('price', 'N/A')} - {first_price.get('fuel_type', 'N/A')}")
                    else:
                        print(f"   📄 Respuesta: {json.dumps(data, indent=2)[:200]}...")
                        
                except json.JSONDecodeError as e:
                    text = await response.text()
                    print(f"   ⚠️  Status: {status} (Respuesta no es JSON)")
                    print(f"   📄 Contenido: {text[:200]}...")
            else:
                text = await response.text()
                print(f"   ❌ Status: {status}")
                print(f"   📄 Error: {text[:200]}...")
                
    except Exception as e:
        print(f"   💥 Error de conexión: {str(e)}")

async def test_all_apis():
    """Prueba todas las APIs principales"""
    print("🚀 Iniciando pruebas de APIs de Gasoradar")
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    async with aiohttp.ClientSession() as session:
        # Lista de endpoints a probar
        endpoints = [
            ("/api/health", "Health check"),
            ("/api/info", "Información de la aplicación"),
            ("/api/v1/gas-stations/statistics/overview", "Estadísticas generales"),
            ("/api/v1/gas-stations/?limit=5", "Lista de gasolineras (5 primeras)"),
            ("/api/v1/gas-stations/?fuel_type=magna&limit=3", "Gasolineras con magna"),
            ("/api/v1/prices/current?limit=5", "Precios actuales"),
            ("/api/v1/prices/statistics?fuel_type=magna", "Estadísticas de precios magna"),
            ("/api/v1/reviews/?limit=3", "Reseñas recientes"),
        ]
        
        for endpoint, description in endpoints:
            await test_api_endpoint(session, endpoint, description)
            await asyncio.sleep(0.5)  # Pausa breve entre requests
    
    print("\n" + "="*60)
    print("✅ Pruebas completadas")
    print("\n💡 Tips para debugging:")
    print("   - Si hay errores 500, revisa los logs del servidor")
    print("   - Si no hay datos, verifica tu conexión a la base de datos")
    print("   - Ejecuta 'python test_db.py' para probar la DB directamente")

async def test_specific_endpoints():
    """Prueba endpoints específicos que podrían estar causando problemas"""
    print("\n🔧 Probando endpoints problemáticos específicos...")
    
    async with aiohttp.ClientSession() as session:
        # Probar con diferentes parámetros
        specific_tests = [
            ("/api/v1/gas-stations/?latitude=19.4326&longitude=-99.1332&radius_km=25&fuel_type=magna", 
             "Gasolineras cerca de CDMX"),
            ("/api/v1/gas-stations/?city=Mexico&fuel_type=magna", 
             "Gasolineras en México DF"),
            ("/api/v1/prices/cheapest?fuel_type=magna&state=Jalisco", 
             "Precios más baratos en Jalisco"),
        ]
        
        for endpoint, description in specific_tests:
            await test_api_endpoint(session, endpoint, description)
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    print("🧪 Gasoradar API Test Suite")
    print("="*60)
    
    try:
        asyncio.run(test_all_apis())
        asyncio.run(test_specific_endpoints())
    except KeyboardInterrupt:
        print("\n⚠️  Pruebas interrumpidas por el usuario")
    except Exception as e:
        print(f"\n💥 Error ejecutando pruebas: {e}")
        
    print(f"\n🏁 Pruebas finalizadas - {datetime.now().strftime('%H:%M:%S')}")