"""
Configuración simplificada de la aplicación Gasoradar - CORREGIDA
"""
import os
from typing import List, Optional
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings

# CARGAR .env EXPLÍCITAMENTE desde la raíz del proyecto
from dotenv import load_dotenv

# Encontrar el archivo .env en la raíz del proyecto
current_file = Path(__file__)  # app/config.py
project_root = current_file.parent.parent  # ../
env_path = project_root / ".env"

# Debug: mostrar dónde está buscando el .env
print(f"🔍 Config loading .env from: {env_path}")
print(f"🔍 .env exists: {env_path.exists()}")

# Cargar el .env
load_dotenv(env_path)

# Debug: verificar que se cargó la DATABASE_URL
database_url_from_env = os.getenv("DATABASE_URL")
print(f"🔍 DATABASE_URL from env: {database_url_from_env[:50] if database_url_from_env else 'NOT FOUND'}...")


class Settings(BaseSettings):
    # ── App ────────────────────────────────────────────
    app_name: str = "Gasoradar"
    app_version: str = "1.0.0"
    debug: bool = Field(default=False, env="DEBUG")
    secret_key: str = Field(default="dev-secret-key", env="SECRET_KEY")

    # ── Database ───────────────────────────────────────
    database_url: str = Field(default="postgresql+asyncpg://localhost/gasoradar", env="DATABASE_URL")
    
    # ── Supabase ───────────────────────────────────────
    supabase_url: str = Field(default="", env="SUPABASE_URL")
    supabase_key: str = Field(default="", env="SUPABASE_KEY")
    supabase_service_key: str = Field(default="", env="SUPABASE_SERVICE_KEY")

    # ── CAPTCHA ────────────────────────────────────────
    recaptcha_site_key: str = Field(default="", env="RECAPTCHA_SITE_KEY")
    recaptcha_secret_key: str = Field(default="", env="RECAPTCHA_SECRET_KEY")

    # ── Maps ───────────────────────────────────────────
    google_maps_api_key: str = Field(default="", env="GOOGLE_MAPS_API_KEY")

    # ── Deployment (campos adicionales que pueden estar en .env) ─────
    allowed_hosts: Optional[str] = Field(default=None, env="ALLOWED_HOSTS")
    port: Optional[int] = Field(default=8000, env="PORT")
    host: Optional[str] = Field(default="0.0.0.0", env="HOST")

    # ── Rate Limiting (protección contra sabotaje) ─────
    price_reports_per_hour: int = 3  # Máximo 3 reportes de precio por IP por hora
    reviews_per_day: int = 2         # Máximo 2 reseñas por IP por día

    # ── Validación dinámica de precios ─────────────────
    price_tolerance_percent: float = 15.0  # ±15% del promedio actual
    min_samples_for_validation: int = 5     # Mínimo de precios para calcular promedio
    price_data_freshness_days: int = 30     # Solo usar precios de últimos 30 días
    
    # Rangos de fallback si no hay datos suficientes
    fallback_price_ranges: dict = {
        "magna": {"min": 15.0, "max": 35.0},
        "premium": {"min": 18.0, "max": 40.0},
        "diesel": {"min": 16.0, "max": 38.0}
    }

    class Config:
        env_file = ".env"
        case_sensitive = False
        # IMPORTANTE: Permitir campos extra del .env
        extra = "ignore"  # Ignora campos adicionales en lugar de dar error


# Instancia global
settings = Settings()

# Debug final: mostrar qué DATABASE_URL está usando
print(f"🔍 Final DATABASE_URL in settings: {settings.database_url[:50]}...")


# Configuración CORS
CORS_SETTINGS = {
    "allow_origins": ["*"],  # En producción, especificar dominios
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}


# Configuración de logging
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
        }
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["default"],
    },
}