#!/usr/bin/env python3
"""
Warhammer Tone Pro Paint Data Generator

Generates the Games Workshop Warhammer Tone Pro range (released 24 October 2026)
from static data. The range is 30 colour families, each sold as five numbered
tones (1 = darkest, 5 = lightest), for 150 paints in 12 ml pots.

Hex values are FauxHammer's samples of Games Workshop's published Tone Pro colour
charts ("The Ultimate Guide To Warhammer Tone Pro", last modified 2026-10-10):
https://www.fauxhammer.com/reviews/paints/ultimate-guide-warhammer-tone-pro/
They are chart samples, not measurements of wet paint. Family names were
cross-checked against Loaded Dice and Tale of Painters.

SKUs and product URLs are left empty because they were not available from a
verifiable source. The two mediums (Shading Medium, Blending Medium) have no
swatch and are not included.

Usage:
    python citadel_tone_pro_generator.py [--output-dir DIR]

Output format matches the standard paint database schema.
"""

import argparse
import json
import re
from pathlib import Path

PRICE_GBP = 4.10

# (family name, colourRange, [tone 1 .. tone 5])  1 = darkest, 5 = lightest
TONE_PRO_FAMILIES = [
    ("Carmine Blades", "Red", ["#3C262A", "#5B3539", "#750019", "#910022", "#C64D50"]),
    ("Bloodtoof Red", "Red", ["#581E16", "#94001E", "#AD072B", "#D60020", "#EF353C"]),
    ("Chotec Orange", "Orange", ["#93150B", "#B82F2D", "#E04A33", "#F56230", "#F78030"]),
    ("Ta’unar Ochre", "Orange", ["#BB5528", "#D27144", "#E78344", "#EA9B6B", "#E9B06E"]),
    ("Khemri Sands", "Yellow", ["#6C4B12", "#8D5E06", "#B1791E", "#CF8400", "#DC9A00"]),
    ("Hawkshroud Yellow", "Yellow", ["#F69113", "#FBA200", "#FEBB00", "#FFD300", "#FDE07E"]),
    ("Megaboss Yellow", "Yellow", ["#F9AD00", "#F8CE00", "#FFE440", "#F8E85F", "#FFF175"]),
    ("Plague Toad Venom", "Green", ["#49892D", "#549B32", "#80C234", "#9CC829", "#C8D600"]),
    ("Kruleboyz Green", "Green", ["#4A7D36", "#627F3C", "#7A823F", "#939859", "#AEB16C"]),
    ("Lustria Green", "Green", ["#004816", "#006929", "#007931", "#34AD40", "#4EB33F"]),
    ("Kasrkin Green", "Green", ["#001B1B", "#1F4530", "#40713C", "#678E55", "#86A978"]),
    ("Hierotek Green", "Green", ["#00321C", "#00542D", "#006A2E", "#00894E", "#00B681"]),
    ("Green Morkshroom", "Green", ["#143230", "#004F48", "#006458", "#007B6D", "#37AC83"]),
    ("Seraphon Blue", "Blue", ["#002F34", "#004650", "#006874", "#008D92", "#00B7B9"]),
    ("Craventhrone Cloak", "Blue", ["#08232E", "#12373E", "#164858", "#41717D", "#598A8E"]),
    ("Stormwolf Blue", "Blue", ["#233F55", "#405568", "#54697D", "#6E95A4", "#87AFB9"]),
    ("Scinari Blue", "Blue", ["#004789", "#005895", "#006C9A", "#0088BB", "#009AB9"]),
    ("Blue Gorkshroom", "Blue", ["#002356", "#004487", "#0057A3", "#006CAD", "#007BB2"]),
    ("Ultramar Blue", "Blue", ["#001A42", "#0F3062", "#0D4F88", "#436CA1", "#6D9ECF"]),
    ("Shalaxi Violet", "Purple", ["#130040", "#262E6D", "#324E99", "#6472B9", "#7E8BC8"]),
    ("Enrapturess Dusk", "Purple", ["#1E1435", "#413549", "#534D5E", "#6E6773", "#817A86"]),
    ("Hive Fleet Purple", "Purple", ["#1E1A40", "#3C1E54", "#655192", "#8769B1", "#8F82AE"]),
    ("Magnus The Red", "Purple", ["#3A202D", "#4A123E", "#76003F", "#9A1B59", "#AA4277"]),
    ("Eye Of Terror", "Pink", ["#920030", "#BD0047", "#C20060", "#D4417B", "#DB6499"]),
    ("Carthalos Skin", "Flesh", ["#241815", "#2D140C", "#4E2212", "#5F3A37", "#795346"]),
    ("Celestine Skin", "Flesh", ["#CD8169", "#D58B70", "#D99578", "#EAA889", "#DDB398"]),
    ("Gnarlwood", "Brown", ["#342928", "#514235", "#725C44", "#776857", "#887B67"]),
    ("Necrolith Bone", "Bone", ["#877561", "#B0916A", "#B99F7D", "#C1AB86", "#BCB79E"]),
    ("Dataslate Grey", "Grey", ["#2B2D28", "#3E3D35", "#555650", "#737069", "#878983"]),
    ("Blackstone Void", "Black", ["#000208", "#4E5458", "#6E7074", "#9B9D9F", "#FFFFFF"]),
]


def slugify(text: str) -> str:
    text = text.lower().replace("\u2019", "").replace("'", "")
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def build_paints() -> list[dict]:
    paints = []
    for family, colour_range, tones in TONE_PRO_FAMILIES:
        assert len(tones) == 5, family
        for tone, hex_value in enumerate(tones, start=1):
            name = f"{family} {tone}"
            paints.append({
                "brand": "Games Workshop",
                "brandData": {},
                "category": "Tone Pro",
                "colourRange": colour_range,
                "currency": "GBP",
                "discontinued": False,
                "hex": hex_value,
                "id": f"citadel-tone-pro-{slugify(name)}",
                "impcat": {"layerId": None, "shadeId": None},
                "name": name,
                "price": PRICE_GBP,
                "range": "Citadel Tone Pro",
                "sku": "",
                "type": "opaque",
                "url": "",
            })
    return sorted(paints, key=lambda p: p["name"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--output-dir", default=str(Path(__file__).parent))
    args = parser.parse_args()
    paints = build_paints()
    out = Path(args.output_dir) / "citadel-tone-pro.json"
    out.write_text(json.dumps(paints, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(paints)} paints to {out}")


if __name__ == "__main__":
    main()
