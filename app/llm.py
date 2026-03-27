from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, Field

from app.config import Settings


class LLMStockPrediction(BaseModel):
    latest_date: str
    direction: str
    confidence: float = Field(ge=0, le=100)
    summary: str
    drivers: list[str]
    risks: list[str]
    latest_day_analysis: str | None = None
    market_relative_analysis: str | None = None
    industry_relative_analysis: str | None = None
    pattern_judgement: str | None = None


class LLMProvider(Protocol):
    def predict_from_bundle(
        self,
        *,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        ...


class OpenAIProvider:
    def __init__(self, *, api_key: str) -> None:
        self.api_key = api_key

    def predict_from_bundle(
        self,
        *,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        request_kwargs = {
            "model": model,
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": prompt,
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"Factor bundle JSON:\n{json.dumps(bundle, ensure_ascii=False)}",
                        }
                    ],
                },
            ],
            "text_format": LLMStockPrediction,
        }
        if reasoning_effort:
            request_kwargs["reasoning"] = {"effort": reasoning_effort}
        response = client.responses.parse(
            **request_kwargs,
        )
        parsed = response.output_parsed
        if parsed is None:
            raise ValueError("OpenAI response did not return structured output")
        return parsed.model_dump()


class OpenAIBatchRunner:
    def __init__(self, *, api_key: str) -> None:
        self.api_key = api_key

    def build_request_line(
        self,
        *,
        custom_id: str,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Factor bundle JSON:\n{json.dumps(bundle, ensure_ascii=False)}"},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "stock_prediction",
                    "strict": True,
                    "schema": LLMStockPrediction.model_json_schema(),
                },
            },
        }
        if reasoning_effort:
            body["reasoning_effort"] = reasoning_effort
        return {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body,
        }

    def submit_batch(self, *, input_file_path: str, completion_window: str = "24h", metadata: dict | None = None) -> dict:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        with Path(input_file_path).expanduser().open("rb") as handle:
            uploaded = client.files.create(file=handle, purpose="batch")
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/chat/completions",
            completion_window=completion_window,
            metadata=metadata or {},
        )
        return {
            "batch_id": batch.id,
            "status": batch.status,
            "input_file_id": uploaded.id,
            "completion_window": completion_window,
        }

    def retrieve_batch(self, *, batch_id: str) -> dict:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        batch = client.batches.retrieve(batch_id)
        return {
            "batch_id": batch.id,
            "status": batch.status,
            "input_file_id": batch.input_file_id,
            "output_file_id": batch.output_file_id,
            "error_file_id": batch.error_file_id,
            "completion_window": batch.completion_window,
        }

    def download_file_text(self, *, file_id: str) -> str:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        content = client.files.content(file_id)
        text = getattr(content, "text", None)
        if callable(text):
            return text()
        if isinstance(text, str):
            return text
        if hasattr(content, "read"):
            raw = content.read()
            return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        raise ValueError(f"Could not decode OpenAI file content for file_id={file_id}")


class GeminiProvider:
    def __init__(self, *, api_key: str) -> None:
        self.api_key = api_key

    def predict_from_bundle(
        self,
        *,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        from google import genai

        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(
            model=model,
            contents=f"Factor bundle JSON:\n{json.dumps(bundle, ensure_ascii=False)}",
            config={
                "system_instruction": prompt,
                "response_mime_type": "application/json",
                "response_json_schema": LLMStockPrediction.model_json_schema(),
            },
        )
        if not response.text:
            raise ValueError("Gemini response did not include text")
        return LLMStockPrediction.model_validate_json(response.text).model_dump()


class GrokProvider:
    def __init__(self, *, api_key: str) -> None:
        self.api_key = api_key

    def predict_from_bundle(
        self,
        *,
        bundle: dict,
        prompt: str,
        model: str,
        reasoning_effort: str | None = None,
    ) -> dict:
        from xai_sdk import Client
        from xai_sdk.chat import system, user

        client = Client(api_key=self.api_key)
        chat = client.chat.create(model=model, response_format=LLMStockPrediction)
        chat.append(system(prompt))
        chat.append(user(f"Factor bundle JSON:\n{json.dumps(bundle, ensure_ascii=False)}"))
        _, parsed = chat.parse(LLMStockPrediction)
        return parsed.model_dump()


def get_llm_provider(*, provider_name: str, settings: Settings) -> LLMProvider:
    normalized = provider_name.lower()
    if normalized in {"openai", "chatgpt"}:
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required for OpenAI/ChatGPT provider")
        return OpenAIProvider(api_key=settings.openai_api_key)
    if normalized == "gemini":
        if not settings.gemini_api_key:
            raise ValueError("GEMINI_API_KEY is required for Gemini provider")
        return GeminiProvider(api_key=settings.gemini_api_key)
    if normalized in {"xai", "grok"}:
        if not settings.xai_api_key:
            raise ValueError("XAI_API_KEY is required for Grok/xAI provider")
        return GrokProvider(api_key=settings.xai_api_key)
    raise ValueError(f"Unsupported provider: {provider_name}")


def get_openai_batch_runner(*, settings: Settings) -> OpenAIBatchRunner:
    if not settings.openai_api_key:
        raise ValueError("OPENAI_API_KEY is required for OpenAI batch workflows")
    return OpenAIBatchRunner(api_key=settings.openai_api_key)


def slugify_model_name(model: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", model).strip("-") or "model"


def load_bundle_inputs(bundle_dir: str) -> dict:
    root = Path(bundle_dir).expanduser()
    return json.loads((root / "bundle.json").read_text(encoding="utf-8"))


def normalize_prompt_provider(provider_name: str) -> str:
    normalized = provider_name.lower()
    if normalized in {"openai", "chatgpt"}:
        return "openai"
    if normalized == "gemini":
        return "gemini"
    if normalized in {"xai", "grok"}:
        return "grok"
    raise ValueError(f"Unsupported provider for prompt selection: {provider_name}")


def prompt_filename(*, provider_name: str, prompt_version: str) -> str:
    provider_key = normalize_prompt_provider(provider_name)
    return f"{provider_key}_stock_eval_{prompt_version}.md"


def load_provider_prompt(*, bundle_dir: str, provider_name: str, prompt_version: str) -> str:
    root = Path(bundle_dir).expanduser()
    prompt_path = root / "prompts" / prompt_filename(provider_name=provider_name, prompt_version=prompt_version)
    if prompt_path.exists():
        return prompt_path.read_text(encoding="utf-8")
    app_prompt_path = Path(__file__).resolve().parent / "prompts" / prompt_filename(
        provider_name=provider_name,
        prompt_version=prompt_version,
    )
    if not app_prompt_path.exists():
        raise ValueError(f"Prompt file not found for provider={provider_name} prompt_version={prompt_version}")
    return app_prompt_path.read_text(encoding="utf-8")
