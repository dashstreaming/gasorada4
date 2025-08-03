#!/usr/bin/env python3
"""
Script para probar la conexión a la base de datos de Gasoradar
"""

import asyncio
import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from app.database import async_session, engine
from app.config import settings
from sqlalchemy import select, text

async def test_database_connection():
    """Prueba la conexión básica a la base de datos"""
    print("🔍 Probando conexión a la base de datos...")
    print(f"🌐 DATABASE_URL: {settings.database_url[:50]}...")
    
    try:
        # Test 1: Conexión básica
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            print(f"✅ Conexión básica exitosa - Test query result: {test_value}")
            
        # Test 2: Verificar versión de PostgreSQL
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"🐘 PostgreSQL version: {version[:100]}...")
            
        # Test 3: Listar tablas existentes
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
            print(f"📋 Tablas encontradas ({len(tables)}): {', '.join(tables)}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        print(f"🔧 Verifica tu DATABASE_URL en el archivo .env")
        return False

async def test_gas_stations_table():
    """Prueba específicamente la tabla gas_stations"""
    print("\n🏪 Probando tabla gas_stations...")
    
    try:
        async with async_session() as session:
            # Verificar si la tabla existe y tiene datos
            result = await session.execute(text("""
                SELECT COUNT(*) as total, 
                       COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coords,
                       COUNT(CASE WHEN is_active = true THEN 1 END) as active
                FROM gas_stations 
                LIMIT 1
            """))
            
            row = result.first()
            if row:
                total, with_coords, active = row
                print(f"📊 Total gasolineras: {total}")
                print(f"🗺️  Con coordenadas: {with_coords}")
                print(f"✅ Activas: {active}")
                
                if total == 0:
                    print("⚠️  La tabla gas_stations está vacía!")
                    return False
                    
                # Mostrar algunas gasolineras de ejemplo
                result = await session.execute(text("""
                    SELECT id, name, city, state, latitude, longitude, is_active
                    FROM gas_stations 
                    WHERE is_active = true
                    LIMIT 5
                """))
                
                print("\n📍 Primeras 5 gasolineras activas:")
                for i, row in enumerate(result.fetchall(), 1):
                    id, name, city, state, lat, lng, active = row
                    print(f"  {i}. {name} - {city}, {state} ({lat:.4f}, {lng:.4f}) - Activa: {active}")
                
                return True
            else:
                print("❌ No se pudo consultar la tabla gas_stations")
                return False
                
    except Exception as e:
        print(f"❌ Error consultando gas_stations: {e}")
        print("\n💡 Posibles problemas:")
        print("   - La tabla 'gas_stations' no existe")
        print("   - Permisos insuficientes para leer la tabla")
        print("   - Estructura de tabla diferente a la esperada")
        return False

async def test_gas_prices_table():
    """Prueba la tabla gas_prices"""
    print("\n💰 Probando tabla gas_prices...")
    
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN is_current = true THEN 1 END) as current_prices,
                       COUNT(DISTINCT fuel_type) as fuel_types
                FROM gas_prices
            """))
            
            row = result.first()
            if row:
                total, current_prices, fuel_types = row
                print(f"📊 Total precios: {total}")
                print(f"⏱️  Precios actuales: {current_prices}")
                print(f"⛽ Tipos de combustible: {fuel_types}")
                
                if total == 0:
                    print("⚠️  La tabla gas_prices está vacía!")
                    return False
                
                # Mostrar tipos de combustible
                result = await session.execute(text("""
                    SELECT fuel_type, COUNT(*) as count
                    FROM gas_prices 
                    WHERE is_current = true
                    GROUP BY fuel_type
                    ORDER BY fuel_type
                """))
                
                print("\n⛽ Distribución por combustible:")
                for fuel_type, count in result.fetchall():
                    print(f"  - {fuel_type}: {count} precios actuales")
                
                return True
            else:
                print("❌ No se pudo consultar la tabla gas_prices")
                return False
                
    except Exception as e:
        print(f"❌ Error consultando gas_prices: {e}")
        return False

async def test_table_relationships():
    """Prueba las relaciones entre tablas"""
    print("\n🔗 Probando relaciones entre tablas...")
    
    try:
        async with async_session() as session:
            result = await session.execute(text("""
                SELECT 
                    gs.name,
                    gs.city,
                    gp.fuel_type,
                    gp.price,
                    gp.created_at
                FROM gas_stations gs
                JOIN gas_prices gp ON gs.id = gp.gas_station_id
                WHERE gs.is_active = true 
                AND gp.is_current = true
                LIMIT 3
            """))
            
            print("🔗 Primeras 3 relaciones gasolinera-precio:")
            for i, row in enumerate(result.fetchall(), 1):
                name, city, fuel_type, price, created_at = row
                print(f"  {i}. {name} ({city}) - {fuel_type}: ${price} - {created_at}")
                
            return True
            
    except Exception as e:
        print(f"❌ Error en relaciones: {e}")
        return False

async def check_environment():
    """Verifica la configuración del entorno"""
    print("🔧 Verificando configuración del entorno...")
    
    # Verificar archivo .env
    env_file = project_root / ".env"
    if env_file.exists():
        print("✅ Archivo .env encontrado")
    else:
        print("⚠️  Archivo .env no encontrado")
        print(f"   Buscado en: {env_file}")
    
    # Verificar variables importantes
    important_vars = [
        'DATABASE_URL',
        'SUPABASE_URL', 
        'SUPABASE_KEY'
    ]
    
    print("\n🔑 Variables de entorno:")
    for var in important_vars:
        value = getattr(settings, var.lower(), None)
        if value:
            # Mostrar solo los primeros caracteres por seguridad
            safe_value = value[:20] + "..." if len(value) > 20 else value
            print(f"  ✅ {var}: {safe_value}")
        else:
            print(f"  ❌ {var}: No configurada")

async def main():
    """Función principal"""
    print("🧪 Test de Base de Datos - Gasoradar")
    print("=" * 60)
    
    # Verificar entorno
    await check_environment()
    
    # Test de conexión
    connection_ok = await test_database_connection()
    
    if not connection_ok:
        print("\n❌ No se puede continuar sin conexión a la base de datos")
        return
    
    # Tests de tablas
    gas_stations_ok = await test_gas_stations_table()
    gas_prices_ok = await test_gas_prices_table()
    
    if gas_stations_ok and gas_prices_ok:
        await test_table_relationships()
    
    print("\n" + "=" * 60)
    print("📋 RESUMEN:")
    print(f"  🔌 Conexión a DB: {'✅' if connection_ok else '❌'}")
    print(f"  🏪 Tabla gas_stations: {'✅' if gas_stations_ok else '❌'}")
    print(f"  💰 Tabla gas_prices: {'✅' if gas_prices_ok else '❌'}")
    
    if connection_ok and gas_stations_ok and gas_prices_ok:
        print("\n🎉 ¡Base de datos funcionando correctamente!")
        print("💡 El problema de los errores 500 puede estar en:")
        print("   - Modelos de SQLAlchemy no coinciden con la estructura real")
        print("   - Imports incorrectos en los servicios")
        print("   - Configuración de async/await")
    else:
        print("\n⚠️  Hay problemas con la base de datos que necesitan resolverse")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Test interrumpido por el usuario")
    except Exception as e:
        print(f"\n💥 Error ejecutando tests: {e}")
        import traceback
        traceback.print_exc()