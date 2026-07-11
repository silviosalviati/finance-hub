from __future__ import annotations

import base64
from functools import lru_cache
from pathlib import Path
from typing import Any

from google.oauth2 import service_account

from src.shared.config import get_runtime_config

_DEFAULT_CREDENTIALS_PATH = str(Path("secrets") / "credentials.json")
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


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


def synthesize_ptbr_mp3(text: str) -> dict[str, Any]:
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

        voice_name = get_runtime_config("FINANCE_AUDITOR_TTS_VOICE", "pt-BR-Chirp3-HD-Achernar")
        speaking_rate = float(get_runtime_config("FINANCE_AUDITOR_TTS_SPEAKING_RATE", "1.0"))

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
        payload = base64.b64encode(response.audio_content or b"").decode("ascii")
        if not payload:
            return {"ok": False, "error": "Servico TTS retornou audio vazio."}

        return {
            "ok": True,
            "mime_type": "audio/mpeg",
            "audio_base64": payload,
            "script": script,
            "voice": voice_name,
            "speaking_rate": speaking_rate,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"Falha ao sintetizar audio: {exc}"}
