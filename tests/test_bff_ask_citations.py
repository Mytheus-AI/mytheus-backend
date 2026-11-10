"""bff `AskCitation` carries `document_id` (company-brain passages) and `start_ms` (transcript
passages) when rag-service's citation has them (wave 3, additive)."""

from __future__ import annotations

import uuid

from services.bff.ask import _citations


def test_document_and_transcript_citations_keep_their_anchor():
    doc, meeting, chunk = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    out = _citations(
        {
            "answer_passages": [
                {
                    "n": 1,
                    "text": "Refunds within 30 days.",
                    "citation": {"kind": "document", "document_id": str(doc), "element_id": "e1"},
                },
                {
                    "n": 2,
                    "text": "We agreed on May.",
                    "citation": {
                        "kind": "transcript",
                        "meeting_id": str(meeting),
                        "chunk_id": str(chunk),
                        "speaker": "Dana",
                        "start_ms": 754_200,
                    },
                    "meeting_title": "Planning",
                },
                {
                    "n": 3,
                    "text": "x",
                    "citation": {"kind": "transcript", "meeting_id": str(meeting), "start_ms": -4},
                },
            ]
        }
    )
    assert out[0].document_id == doc and out[0].start_ms is None and out[0].meeting_id is None
    assert out[1].start_ms == 754_200 and out[1].document_id is None
    assert out[1].meeting_title == "Planning"
    assert out[2].start_ms is None
