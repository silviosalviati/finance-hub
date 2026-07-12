from unittest.mock import MagicMock, patch

from src.shared.tools import tts


def test_get_or_create_voice_preview_genero_invalido():
    result = tts.get_or_create_voice_preview("neutra")
    assert result["ok"] is False
    assert "inválido" in result["error"]


def test_get_or_create_voice_preview_reusa_cache_existente(tmp_path):
    cached = tmp_path / "preview_feminina.mp3"
    cached.write_bytes(b"fake-mp3")

    with (
        patch.object(tts, "_PODCAST_ROOT", tmp_path),
        patch.object(tts, "synthesize_ptbr_mp3") as fake_synth,
    ):
        result = tts.get_or_create_voice_preview("feminina")

    fake_synth.assert_not_called()
    assert result == {"ok": True, "mime_type": "audio/mpeg", "audio_path": str(cached)}


def test_get_or_create_voice_preview_gera_quando_nao_ha_cache(tmp_path):
    with (
        patch.object(tts, "_PODCAST_ROOT", tmp_path),
        patch.object(
            tts, "synthesize_ptbr_mp3", return_value={"ok": True, "mime_type": "audio/mpeg"}
        ) as fake_synth,
    ):
        result = tts.get_or_create_voice_preview("masculina")

    fake_synth.assert_called_once()
    assert fake_synth.call_args.args[0] == tts._PREVIEW_TEXT
    assert fake_synth.call_args.kwargs["asset_id"] == "preview_masculina"
    assert fake_synth.call_args.kwargs["gender"] == "masculina"
    assert result["ok"] is True


def test_resolve_voice_name_usa_voz_configurada_por_genero():
    config = {
        "FINANCE_AUDITOR_TTS_VOICE": "pt-BR-Chirp3-HD-Achernar",
        "FINANCE_AUDITOR_TTS_VOICE_MASCULINA": "pt-BR-Chirp3-HD-Orus",
        "FINANCE_AUDITOR_TTS_VOICE_FEMININA": "pt-BR-Chirp3-HD-Vindemiatrix",
    }
    with patch.object(tts, "get_runtime_config", side_effect=lambda key, default="": config.get(key, default)):
        assert tts._resolve_voice_name("masculina") == "pt-BR-Chirp3-HD-Orus"
        assert tts._resolve_voice_name("feminina") == "pt-BR-Chirp3-HD-Vindemiatrix"


def test_resolve_voice_name_cai_no_fallback_quando_genero_nao_configurado():
    config = {"FINANCE_AUDITOR_TTS_VOICE": "pt-BR-Chirp3-HD-Achernar"}
    with patch.object(tts, "get_runtime_config", side_effect=lambda key, default="": config.get(key, default)):
        assert tts._resolve_voice_name("masculina") == "pt-BR-Chirp3-HD-Achernar"
        assert tts._resolve_voice_name("feminina") == "pt-BR-Chirp3-HD-Achernar"


def test_resolve_voice_name_genero_invalido_cai_no_fallback():
    config = {"FINANCE_AUDITOR_TTS_VOICE": "pt-BR-Chirp3-HD-Achernar"}
    with patch.object(tts, "get_runtime_config", side_effect=lambda key, default="": config.get(key, default)):
        assert tts._resolve_voice_name("neutra") == "pt-BR-Chirp3-HD-Achernar"
        assert tts._resolve_voice_name(None) == "pt-BR-Chirp3-HD-Achernar"


def test_clean_markdown_for_speech_preserva_paragrafos():
    text = "Primeira linha.\nSegunda linha.\n\nNovo paragrafo.\n- item de lista"
    cleaned = tts._clean_markdown_for_speech(text)
    assert cleaned == "Primeira linha.\nSegunda linha.\n\nNovo paragrafo.\nitem de lista"


def test_build_ssml_agrupa_paragrafos_e_frases_e_escapa_xml():
    script = "Primeira frase.\nSegunda frase.\n\nTaxa acima de 5 & 10%."
    ssml = tts._build_ssml(script)
    assert ssml == (
        "<speak>"
        "<p><s>Primeira frase.</s><s>Segunda frase.</s></p>"
        "<p><s>Taxa acima de 5 &amp; 10%.</s></p>"
        "</speak>"
    )


def test_synthesize_ptbr_mp3_envia_ssml_para_o_cliente_tts(tmp_path):
    fake_response = MagicMock(audio_content=b"fake-mp3-bytes")
    fake_client = MagicMock()
    fake_client.synthesize_speech.return_value = fake_response

    with (
        patch.object(tts, "_PODCAST_ROOT", tmp_path),
        patch.object(tts, "_get_credentials", return_value=MagicMock()),
        patch("google.cloud.texttospeech.TextToSpeechClient", return_value=fake_client),
    ):
        result = tts.synthesize_ptbr_mp3(
            "Achado principal.\n\nRecomendacao final.", asset_id="abc", gender="feminina"
        )

    assert result["ok"] is True
    request = fake_client.synthesize_speech.call_args.kwargs["request"]
    assert request["input"].ssml == (
        "<speak><p><s>Achado principal.</s></p><p><s>Recomendacao final.</s></p></speak>"
    )
    assert request["input"].text == ""
