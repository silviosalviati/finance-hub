from __future__ import annotations

import uuid
from functools import lru_cache
from pathlib import Path
from typing import Any

from google.oauth2 import service_account

from src.shared.config import get_runtime_config

_DEFAULT_CREDENTIALS_PATH = str(Path("secrets") / "credentials.json")
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_PODCAST_ROOT = _PROJECT_ROOT / ".sixth" / "finance_podcasts"


def _resolve_credentials_path(credentials_path: str | None) -> str:
    configured = (credentials_path or "").strip() or _DEFAULT_CREDENTIALS_PATH
    candidate = Path(configured).expanduser()

    if candidate.is_absolute():
        return str(candidate)
    return str((_PROJECT_ROOT / candidate).resolve())


@lru_cache(maxsize=2)
def _get_credentials(credentials_path: str):
    return service_account.Credentials.from_service_account_file(credentials_path)


def _clean_markdown_for_speech(text: str) -> str:
    cleaned = text or ""
    cleaned = cleaned.replace("\r", "")
    # Remove cercas de codigo e links markdown, mantendo o texto legivel.
    cleaned = cleaned.replace("```", "")
    cleaned = cleaned.replace("`", "")
    cleaned = cleaned.replace("**", "")
    cleaned = cleaned.replace("*", "")
    cleaned = cleaned.replace("#", "")
    cleaned = cleaned.replace("[", "")
    cleaned = cleaned.replace("]", "")
    cleaned = cleaned.replace("(", "")
    cleaned = cleaned.replace(")", "")

    lines = []
    for line in cleaned.split("\n"):
        s = line.strip()
        if not s:
            continue
        if s.startswith("-"):
            s = s[1:].strip()
        lines.append(s)
    return "\n".join(lines).strip()


def build_podcast_script(text: str, max_chars: int = 3500) -> str:
    script = _clean_markdown_for_speech(text)
    if len(script) > max_chars:
        return script[:max_chars].rstrip() + "..."
    return script


def _resolve_voice_name(gender: str | None) -> str:
    gender_key = str(gender or "").strip().lower()
    gender_map = {
        "masculina": "FINANCE_AUDITOR_TTS_VOICE_MASCULINA",
        "feminina": "FINANCE_AUDITOR_TTS_VOICE_FEMININA",
    }
    fallback = get_runtime_config("FINANCE_AUDITOR_TTS_VOICE", "pt-BR-Chirp3-HD-Achernar")
    key = gender_map.get(gender_key)
    if not key:
        return fallback
    candidate = get_runtime_config(key, "").strip()
    return candidate or fallback


# Frase curta e genérica (não depende de nenhuma análise) usada só pra dar
# ao usuário uma amostra de como cada voz soa antes de gerar o podcast de
# verdade — ver get_or_create_voice_preview.
_PREVIEW_TEXT = "Olá! Esta é a narração do Finance Voice para o seu podcast."


def get_or_create_voice_preview(gender: str) -> dict[str, Any]:
    """Amostra curta e cacheada de uma voz, pra prévia no HITL do podcast.

    Sintetiza a MESMA frase fixa uma única vez por gênero (asset_id
    determinístico = mesmo caminho em disco sempre) — qualquer clique
    seguinte, de qualquer usuário, só reproduz o arquivo já gerado, sem
    gastar TTS de novo.
    """
    gender_key = str(gender or "").strip().lower()
    if gender_key not in ("masculina", "feminina"):
        return {"ok": False, "error": f"Gênero de voz inválido: {gender!r}."}

    asset_id = f"preview_{gender_key}"
    cached_path = _PODCAST_ROOT / f"{asset_id}.mp3"
    if cached_path.exists():
        return {"ok": True, "mime_type": "audio/mpeg", "audio_path": str(cached_path)}

    return synthesize_ptbr_mp3(_PREVIEW_TEXT, asset_id=asset_id, gender=gender_key)


def synthesize_ptbr_mp3(text: str, asset_id: str | None = None, gender: str | None = None) -> dict[str, Any]:
    script = build_podcast_script(text)
    if not script:
        return {"ok": False, "error": "Texto vazio para sintetizar audio."}

    try:
        from google.cloud import texttospeech
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": (
                "SDK de Text-to-Speech indisponivel no ambiente "
                f"(google-cloud-texttospeech): {exc}"
            ),
        }

    credentials_path = _resolve_credentials_path(
        get_runtime_config("GOOGLE_APPLICATION_CREDENTIALS", _DEFAULT_CREDENTIALS_PATH)
    )

    try:
        creds = _get_credentials(credentials_path)
        client = texttospeech.TextToSpeechClient(credentials=creds)

        voice_name = _resolve_voice_name(gender)
        speaking_rate = float(get_runtime_config("FINANCE_AUDITOR_TTS_SPEAKING_RATE", "1.0"))
        max_bytes = int(get_runtime_config("FINANCE_AUDITOR_PODCAST_MAX_BYTES", "20000000"))
        resolved_asset_id = (asset_id or uuid.uuid4().hex).strip() or uuid.uuid4().hex

        response = client.synthesize_speech(
            request={
                "input": texttospeech.SynthesisInput(text=script),
                "voice": texttospeech.VoiceSelectionParams(
                    language_code="pt-BR",
                    name=voice_name,
                ),
                "audio_config": texttospeech.AudioConfig(
                    audio_encoding=texttospeech.AudioEncoding.MP3,
                    speaking_rate=speaking_rate,
                ),
            }
        )
        audio_content = bytes(response.audio_content or b"")
        if not audio_content:
            return {"ok": False, "error": "Servico TTS retornou audio vazio."}
        if max_bytes > 0 and len(audio_content) > max_bytes:
            return {
                "ok": False,
                "error": (
                    "Audio gerado excedeu o limite configurado para podcast "
                    f"({len(audio_content)} > {max_bytes} bytes)."
                ),
            }

        _PODCAST_ROOT.mkdir(parents=True, exist_ok=True)
        audio_path = _PODCAST_ROOT / f"{resolved_asset_id}.mp3"
        audio_path.write_bytes(audio_content)

        return {
            "ok": True,
            "mime_type": "audio/mpeg",
            "audio_path": str(audio_path),
            "audio_size_bytes": len(audio_content),
            "audio_id": resolved_asset_id,
            "script": script,
            "voice": voice_name,
            "speaking_rate": speaking_rate,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"Falha ao sintetizar audio: {exc}"}
