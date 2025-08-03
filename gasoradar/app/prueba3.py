#!/usr/bin/env python3
"""
Script simple para probar la conexión a la base de datos
"""

import asyncio
import sys
import os
from pathlib import Path

# Cambiar al directorio padre para importar correctamente
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
os.chdir(parent_dir)

print(f"📁 Directorio de trabajo: {os.getcwd()}")

try:
    # Importar configuración
    print("🔧 Cargando configuración...")
    from app.config import settings
    print(f"✅ Configuración cargada")
    print(f"🌐 DATABASE_URL: {settings.database_url[:50]}...")
    
    # Importar base de datos
    print("🔌 Importando módulos de base de datos...")
    from app.database import async_session, engine
    print("✅ Módulos de DB importados")
    
    # Importar SQLAlchemy
    from sqlalchemy import select, text
    print("✅ SQLAlchemy importado")
    
except Exception as e:
    print(f"❌ Error en imports: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

async def test_basic_connection():
    """Prueba conexión básica"""
    print("\n🔍 Probando conexión básica...")
    
    try:
        # Test con engine directamente
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            print(f"✅ Conexión básica exitosa - Test: {test_value}")
            return True
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        print("\n💡 Posibles causas:")
        print("   - DATABASE_URL incorrecta")
        print("   - Base de datos no accesible")
        print("   - Credenciales incorrectas")
        return False

async def test_tables():
    """Verifica si existen las tablas"""
    print("\n📋 Verificando tablas...")
    
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
            
            print(f"📊 Tablas encontradas ({len(tables)}):")
            for table in tables:
                print(f"   - {table}")
            
            # Verificar tablas específicas que necesitamos
            required_tables = ['gas_stations', 'gas_prices']
            missing_tables = [t for t in required_tables if t not in tables]
            
            if missing_tables:
                print(f"⚠️  Tablas faltantes: {missing_tables}")
                return False
            else:
                print("✅ Todas las tablas requeridas existen")
                return True
                
    except Exception as e:
        print(f"❌ Error verificando tablas: {e}")
        return False

async def test_data():
    """Verifica si hay datos en las tablas"""
    print("\n📊 Verificando datos...")
    
    try:
        async with async_session() as session:
            # Contar gasolineras
            result = await session.execute(text("SELECT COUNT(*) FROM gas_stations"))
            gas_stations_count = result.scalar()
            print(f"🏪 Gasolineras: {gas_stations_count}")
            
            # Contar precios
            result = await session.execute(text("SELECT COUNT(*) FROM gas_prices"))
            prices_count = result.scalar()
            print(f"💰 Precios: {prices_count}")
            
            if gas_stations_count == 0:
                print("⚠️  No hay gasolineras en la base de datos")
                return False
            
            if prices_count == 0:
                print("⚠️  No hay precios en la base de datos")
                return False
            
            # Mostrar una gasolinera de ejemplo
            result = await session.execute(text("""
                SELECT name, city, state 
                FROM gas_stations 
                WHERE is_active = true 
                LIMIT 1
            """))
            station = result.first()
            
            if station:
                print(f"📍 Ejemplo: {station[0]} - {station[1]}, {station[2]}")
            
            return True
            
    except Exception as e:
        print(f"❌ Error verificando datos: {e}")
        return False

async def main():
    """Función principal"""
    print("🧪 Test Simple de Base de Datos - Gasoradar")
    print("=" * 50)
    
    # Test 1: Conexión básica
    connection_ok = await test_basic_connection()
    
    if not connection_ok:
        print("\n❌ No se puede continuar sin conexión")
        return False
    
    # Test 2: Verificar tablas
    tables_ok = await test_tables()
    
    if not tables_ok:
        print("\n❌ Faltan tablas en la base de datos")
        return False
    
    # Test 3: Verificar datos
    data_ok = await test_data()
    
    print("\n" + "=" * 50)
    print("📋 RESUMEN:")
    print(f"  🔌 Conexión: {'✅' if connection_ok else '❌'}")
    print(f"  📋 Tablas: {'✅' if tables_ok else '❌'}")
    print(f"  📊 Datos: {'✅' if data_ok else '❌'}")
    
    if connection_ok and tables_ok and data_ok:
        print("\n🎉 ¡Base de datos funcionando correctamente!")
        print("🔧 Si aún hay errores 500, el problema está en el código de la API")
    else:
        print("\n⚠️  Hay problemas que necesitan resolverse")
    
    return connection_ok and tables_ok and data_ok

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        if result:
            print("\n💡 Próximo paso: Verificar los modelos de SQLAlchemy")
        else:
            print("\n💡 Próximo paso: Solucionar problemas de base de datos")
    except KeyboardInterrupt:
        print("\n⚠️  Test interrumpido")
    except Exception as e:
        print(f"\n💥 Error ejecutando test: {e}")
        import traceback
        traceback.print_exc()