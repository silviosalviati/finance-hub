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


def _resolve_voice_name(persona: str | None) -> str:
    persona_key = str(persona or "").strip().lower()
    persona_map = {
        "coordenador": "FINANCE_AUDITOR_TTS_VOICE_COORDENADOR",
        "gerente": "FINANCE_AUDITOR_TTS_VOICE_GERENTE",
        "diretor": "FINANCE_AUDITOR_TTS_VOICE_DIRETOR",
        "geral": "FINANCE_AUDITOR_TTS_VOICE_GERAL",
    }
    fallback = get_runtime_config("FINANCE_AUDITOR_TTS_VOICE", "pt-BR-Chirp3-HD-Achernar")
    key = persona_map.get(persona_key)
    if not key:
        return fallback
    candidate = get_runtime_config(key, "").strip()
    return candidate or fallback


def synthesize_ptbr_mp3(text: str, asset_id: str | None = None, persona: str | None = None) -> dict[str, Any]:
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

        voice_name = _resolve_voice_name(persona)
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
