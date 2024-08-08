use std::collections::HashMap;

use lazy_static::lazy_static;

#[derive(Debug)]
pub struct Collation {
    pub name: &'static str,
    pub charset: &'static str,
    pub id: u32,
    pub default: bool,
}

lazy_static! {

    /// see INFORMATION_SCHEMA.COLLATIONS for more details
    static ref COLLMAP: HashMap<u32, Collation> = {
        let mut map = HashMap::new();
        map.insert(
            32,
            Collation {
                name: "armscii8_general_ci",
                charset: "armscii8",
                id: 32,
                default: true,
            },
        );
        map.insert(
            64,
            Collation {
                name: "armscii8_bin",
                charset: "armscii8",
                id: 64,
                default: false,
            },
        );
        map.insert(
            11,
            Collation {
                name: "ascii_general_ci",
                charset: "ascii",
                id: 11,
                default: true,
            },
        );
        map.insert(
            65,
            Collation {
                name: "ascii_bin",
                charset: "ascii",
                id: 65,
                default: false,
            },
        );
        map.insert(
            1,
            Collation {
                name: "big5_chinese_ci",
                charset: "big5",
                id: 1,
                default: true,
            },
        );
        map.insert(
            84,
            Collation {
                name: "big5_bin",
                charset: "big5",
                id: 84,
                default: false,
            },
        );
        map.insert(
            63,
            Collation {
                name: "binary",
                charset: "binary",
                id: 63,
                default: true,
            },
        );
        map.insert(
            26,
            Collation {
                name: "cp1250_general_ci",
                charset: "cp1250",
                id: 26,
                default: true,
            },
        );
        map.insert(
            34,
            Collation {
                name: "cp1250_czech_cs",
                charset: "cp1250",
                id: 34,
                default: false,
            },
        );
        map.insert(
            44,
            Collation {
                name: "cp1250_croatian_ci",
                charset: "cp1250",
                id: 44,
                default: false,
            },
        );
        map.insert(
            66,
            Collation {
                name: "cp1250_bin",
                charset: "cp1250",
                id: 66,
                default: false,
            },
        );
        map.insert(
            99,
            Collation {
                name: "cp1250_polish_ci",
                charset: "cp1250",
                id: 99,
                default: false,
            },
        );
        map.insert(
            14,
            Collation {
                name: "cp1251_bulgarian_ci",
                charset: "cp1251",
                id: 14,
                default: false,
            },
        );
        map.insert(
            23,
            Collation {
                name: "cp1251_ukrainian_ci",
                charset: "cp1251",
                id: 23,
                default: false,
            },
        );
        map.insert(
            50,
            Collation {
                name: "cp1251_bin",
                charset: "cp1251",
                id: 50,
                default: false,
            },
        );
        map.insert(
            51,
            Collation {
                name: "cp1251_general_ci",
                charset: "cp1251",
                id: 51,
                default: true,
            },
        );
        map.insert(
            52,
            Collation {
                name: "cp1251_general_cs",
                charset: "cp1251",
                id: 52,
                default: false,
            },
        );
        map.insert(
            57,
            Collation {
                name: "cp1256_general_ci",
                charset: "cp1256",
                id: 57,
                default: true,
            },
        );
        map.insert(
            67,
            Collation {
                name: "cp1256_bin",
                charset: "cp1256",
                id: 67,
                default: false,
            },
        );
        map.insert(
            29,
            Collation {
                name: "cp1257_lithuanian_ci",
                charset: "cp1257",
                id: 29,
                default: false,
            },
        );
        map.insert(
            58,
            Collation {
                name: "cp1257_bin",
                charset: "cp1257",
                id: 58,
                default: false,
            },
        );
        map.insert(
            59,
            Collation {
                name: "cp1257_general_ci",
                charset: "cp1257",
                id: 59,
                default: true,
            },
        );
        map.insert(
            4,
            Collation {
                name: "cp850_general_ci",
                charset: "cp850",
                id: 4,
                default: true,
            },
        );
        map.insert(
            80,
            Collation {
                name: "cp850_bin",
                charset: "cp850",
                id: 80,
                default: false,
            },
        );
        map.insert(
            40,
            Collation {
                name: "cp852_general_ci",
                charset: "cp852",
                id: 40,
                default: true,
            },
        );
        map.insert(
            81,
            Collation {
                name: "cp852_bin",
                charset: "cp852",
                id: 81,
                default: false,
            },
        );
        map.insert(
            36,
            Collation {
                name: "cp866_general_ci",
                charset: "cp866",
                id: 36,
                default: true,
            },
        );
        map.insert(
            68,
            Collation {
                name: "cp866_bin",
                charset: "cp866",
                id: 68,
                default: false,
            },
        );
        map.insert(
            95,
            Collation {
                name: "cp932_japanese_ci",
                charset: "cp932",
                id: 95,
                default: true,
            },
        );
        map.insert(
            96,
            Collation {
                name: "cp932_bin",
                charset: "cp932",
                id: 96,
                default: false,
            },
        );
        map.insert(
            3,
            Collation {
                name: "dec8_swedish_ci",
                charset: "dec8",
                id: 3,
                default: true,
            },
        );
        map.insert(
            69,
            Collation {
                name: "dec8_bin",
                charset: "dec8",
                id: 69,
                default: false,
            },
        );
        map.insert(
            97,
            Collation {
                name: "eucjpms_japanese_ci",
                charset: "eucjpms",
                id: 97,
                default: true,
            },
        );
        map.insert(
            98,
            Collation {
                name: "eucjpms_bin",
                charset: "eucjpms",
                id: 98,
                default: false,
            },
        );
        map.insert(
            19,
            Collation {
                name: "euckr_korean_ci",
                charset: "euckr",
                id: 19,
                default: true,
            },
        );
        map.insert(
            85,
            Collation {
                name: "euckr_bin",
                charset: "euckr",
                id: 85,
                default: false,
            },
        );
        map.insert(
            248,
            Collation {
                name: "gb18030_chinese_ci",
                charset: "gb18030",
                id: 248,
                default: true,
            },
        );
        map.insert(
            249,
            Collation {
                name: "gb18030_bin",
                charset: "gb18030",
                id: 249,
                default: false,
            },
        );
        map.insert(
            250,
            Collation {
                name: "gb18030_unicode_520_ci",
                charset: "gb18030",
                id: 250,
                default: false,
            },
        );
        map.insert(
            24,
            Collation {
                name: "gb2312_chinese_ci",
                charset: "gb2312",
                id: 24,
                default: true,
            },
        );
        map.insert(
            86,
            Collation {
                name: "gb2312_bin",
                charset: "gb2312",
                id: 86,
                default: false,
            },
        );
        map.insert(
            28,
            Collation {
                name: "gbk_chinese_ci",
                charset: "gbk",
                id: 28,
                default: true,
            },
        );
        map.insert(
            87,
            Collation {
                name: "gbk_bin",
                charset: "gbk",
                id: 87,
                default: false,
            },
        );
        map.insert(
            92,
            Collation {
                name: "geostd8_general_ci",
                charset: "geostd8",
                id: 92,
                default: true,
            },
        );
        map.insert(
            93,
            Collation {
                name: "geostd8_bin",
                charset: "geostd8",
                id: 93,
                default: false,
            },
        );
        map.insert(
            25,
            Collation {
                name: "greek_general_ci",
                charset: "greek",
                id: 25,
                default: true,
            },
        );
        map.insert(
            70,
            Collation {
                name: "greek_bin",
                charset: "greek",
                id: 70,
                default: false,
            },
        );
        map.insert(
            16,
            Collation {
                name: "hebrew_general_ci",
                charset: "hebrew",
                id: 16,
                default: true,
            },
        );
        map.insert(
            71,
            Collation {
                name: "hebrew_bin",
                charset: "hebrew",
                id: 71,
                default: false,
            },
        );
        map.insert(
            6,
            Collation {
                name: "hp8_english_ci",
                charset: "hp8",
                id: 6,
                default: true,
            },
        );
        map.insert(
            72,
            Collation {
                name: "hp8_bin",
                charset: "hp8",
                id: 72,
                default: false,
            },
        );
        map.insert(
            37,
            Collation {
                name: "keybcs2_general_ci",
                charset: "keybcs2",
                id: 37,
                default: true,
            },
        );
        map.insert(
            73,
            Collation {
                name: "keybcs2_bin",
                charset: "keybcs2",
                id: 73,
                default: false,
            },
        );
        map.insert(
            7,
            Collation {
                name: "koi8r_general_ci",
                charset: "koi8r",
                id: 7,
                default: true,
            },
        );
        map.insert(
            74,
            Collation {
                name: "koi8r_bin",
                charset: "koi8r",
                id: 74,
                default: false,
            },
        );
        map.insert(
            22,
            Collation {
                name: "koi8u_general_ci",
                charset: "koi8u",
                id: 22,
                default: true,
            },
        );
        map.insert(
            75,
            Collation {
                name: "koi8u_bin",
                charset: "koi8u",
                id: 75,
                default: false,
            },
        );
        map.insert(
            5,
            Collation {
                name: "latin1_german1_ci",
                charset: "latin1",
                id: 5,
                default: false,
            },
        );
        map.insert(
            8,
            Collation {
                name: "latin1_swedish_ci",
                charset: "latin1",
                id: 8,
                default: true,
            },
        );
        map.insert(
            15,
            Collation {
                name: "latin1_danish_ci",
                charset: "latin1",
                id: 15,
                default: false,
            },
        );
        map.insert(
            31,
            Collation {
                name: "latin1_german2_ci",
                charset: "latin1",
                id: 31,
                default: false,
            },
        );
        map.insert(
            47,
            Collation {
                name: "latin1_bin",
                charset: "latin1",
                id: 47,
                default: false,
            },
        );
        map.insert(
            48,
            Collation {
                name: "latin1_general_ci",
                charset: "latin1",
                id: 48,
                default: false,
            },
        );
        map.insert(
            49,
            Collation {
                name: "latin1_general_cs",
                charset: "latin1",
                id: 49,
                default: false,
            },
        );
        map.insert(
            94,
            Collation {
                name: "latin1_spanish_ci",
                charset: "latin1",
                id: 94,
                default: false,
            },
        );
        map.insert(
            2,
            Collation {
                name: "latin2_czech_cs",
                charset: "latin2",
                id: 2,
                default: false,
            },
        );
        map.insert(
            9,
            Collation {
                name: "latin2_general_ci",
                charset: "latin2",
                id: 9,
                default: true,
            },
        );
        map.insert(
            21,
            Collation {
                name: "latin2_hungarian_ci",
                charset: "latin2",
                id: 21,
                default: false,
            },
        );
        map.insert(
            27,
            Collation {
                name: "latin2_croatian_ci",
                charset: "latin2",
                id: 27,
                default: false,
            },
        );
        map.insert(
            77,
            Collation {
                name: "latin2_bin",
                charset: "latin2",
                id: 77,
                default: false,
            },
        );
        map.insert(
            30,
            Collation {
                name: "latin5_turkish_ci",
                charset: "latin5",
                id: 30,
                default: true,
            },
        );
        map.insert(
            78,
            Collation {
                name: "latin5_bin",
                charset: "latin5",
                id: 78,
                default: false,
            },
        );
        map.insert(
            20,
            Collation {
                name: "latin7_estonian_cs",
                charset: "latin7",
                id: 20,
                default: false,
            },
        );
        map.insert(
            41,
            Collation {
                name: "latin7_general_ci",
                charset: "latin7",
                id: 41,
                default: true,
            },
        );
        map.insert(
            42,
            Collation {
                name: "latin7_general_cs",
                charset: "latin7",
                id: 42,
                default: false,
            },
        );
        map.insert(
            79,
            Collation {
                name: "latin7_bin",
                charset: "latin7",
                id: 79,
                default: false,
            },
        );
        map.insert(
            38,
            Collation {
                name: "macce_general_ci",
                charset: "macce",
                id: 38,
                default: true,
            },
        );
        map.insert(
            43,
            Collation {
                name: "macce_bin",
                charset: "macce",
                id: 43,
                default: false,
            },
        );
        map.insert(
            39,
            Collation {
                name: "macroman_general_ci",
                charset: "macroman",
                id: 39,
                default: true,
            },
        );
        map.insert(
            53,
            Collation {
                name: "macroman_bin",
                charset: "macroman",
                id: 53,
                default: false,
            },
        );
        map.insert(
            13,
            Collation {
                name: "sjis_japanese_ci",
                charset: "sjis",
                id: 13,
                default: true,
            },
        );
        map.insert(
            88,
            Collation {
                name: "sjis_bin",
                charset: "sjis",
                id: 88,
                default: false,
            },
        );
        map.insert(
            10,
            Collation {
                name: "swe7_swedish_ci",
                charset: "swe7",
                id: 10,
                default: true,
            },
        );
        map.insert(
            82,
            Collation {
                name: "swe7_bin",
                charset: "swe7",
                id: 82,
                default: false,
            },
        );
        map.insert(
            18,
            Collation {
                name: "tis620_thai_ci",
                charset: "tis620",
                id: 18,
                default: true,
            },
        );
        map.insert(
            89,
            Collation {
                name: "tis620_bin",
                charset: "tis620",
                id: 89,
                default: false,
            },
        );
        map.insert(
            35,
            Collation {
                name: "ucs2_general_ci",
                charset: "ucs2",
                id: 35,
                default: true,
            },
        );
        map.insert(
            90,
            Collation {
                name: "ucs2_bin",
                charset: "ucs2",
                id: 90,
                default: false,
            },
        );
        map.insert(
            128,
            Collation {
                name: "ucs2_unicode_ci",
                charset: "ucs2",
                id: 128,
                default: false,
            },
        );
        map.insert(
            129,
            Collation {
                name: "ucs2_icelandic_ci",
                charset: "ucs2",
                id: 129,
                default: false,
            },
        );
        map.insert(
            130,
            Collation {
                name: "ucs2_latvian_ci",
                charset: "ucs2",
                id: 130,
                default: false,
            },
        );
        map.insert(
            131,
            Collation {
                name: "ucs2_romanian_ci",
                charset: "ucs2",
                id: 131,
                default: false,
            },
        );
        map.insert(
            132,
            Collation {
                name: "ucs2_slovenian_ci",
                charset: "ucs2",
                id: 132,
                default: false,
            },
        );
        map.insert(
            133,
            Collation {
                name: "ucs2_polish_ci",
                charset: "ucs2",
                id: 133,
                default: false,
            },
        );
        map.insert(
            134,
            Collation {
                name: "ucs2_estonian_ci",
                charset: "ucs2",
                id: 134,
                default: false,
            },
        );
        map.insert(
            135,
            Collation {
                name: "ucs2_spanish_ci",
                charset: "ucs2",
                id: 135,
                default: false,
            },
        );
        map.insert(
            136,
            Collation {
                name: "ucs2_swedish_ci",
                charset: "ucs2",
                id: 136,
                default: false,
            },
        );
        map.insert(
            137,
            Collation {
                name: "ucs2_turkish_ci",
                charset: "ucs2",
                id: 137,
                default: false,
            },
        );
        map.insert(
            138,
            Collation {
                name: "ucs2_czech_ci",
                charset: "ucs2",
                id: 138,
                default: false,
            },
        );
        map.insert(
            139,
            Collation {
                name: "ucs2_danish_ci",
                charset: "ucs2",
                id: 139,
                default: false,
            },
        );
        map.insert(
            140,
            Collation {
                name: "ucs2_lithuanian_ci",
                charset: "ucs2",
                id: 140,
                default: false,
            },
        );
        map.insert(
            141,
            Collation {
                name: "ucs2_slovak_ci",
                charset: "ucs2",
                id: 141,
                default: false,
            },
        );
        map.insert(
            142,
            Collation {
                name: "ucs2_spanish2_ci",
                charset: "ucs2",
                id: 142,
                default: false,
            },
        );
        map.insert(
            143,
            Collation {
                name: "ucs2_roman_ci",
                charset: "ucs2",
                id: 143,
                default: false,
            },
        );
        map.insert(
            144,
            Collation {
                name: "ucs2_persian_ci",
                charset: "ucs2",
                id: 144,
                default: false,
            },
        );
        map.insert(
            145,
            Collation {
                name: "ucs2_esperanto_ci",
                charset: "ucs2",
                id: 145,
                default: false,
            },
        );
        map.insert(
            146,
            Collation {
                name: "ucs2_hungarian_ci",
                charset: "ucs2",
                id: 146,
                default: false,
            },
        );
        map.insert(
            147,
            Collation {
                name: "ucs2_sinhala_ci",
                charset: "ucs2",
                id: 147,
                default: false,
            },
        );
        map.insert(
            148,
            Collation {
                name: "ucs2_german2_ci",
                charset: "ucs2",
                id: 148,
                default: false,
            },
        );
        map.insert(
            149,
            Collation {
                name: "ucs2_croatian_ci",
                charset: "ucs2",
                id: 149,
                default: false,
            },
        );
        map.insert(
            150,
            Collation {
                name: "ucs2_unicode_520_ci",
                charset: "ucs2",
                id: 150,
                default: false,
            },
        );
        map.insert(
            151,
            Collation {
                name: "ucs2_vietnamese_ci",
                charset: "ucs2",
                id: 151,
                default: false,
            },
        );
        map.insert(
            159,
            Collation {
                name: "ucs2_general_mysql500_ci",
                charset: "ucs2",
                id: 159,
                default: false,
            },
        );
        map.insert(
            12,
            Collation {
                name: "ujis_japanese_ci",
                charset: "ujis",
                id: 12,
                default: true,
            },
        );
        map.insert(
            91,
            Collation {
                name: "ujis_bin",
                charset: "ujis",
                id: 91,
                default: false,
            },
        );
        map.insert(
            54,
            Collation {
                name: "utf16_general_ci",
                charset: "utf16",
                id: 54,
                default: true,
            },
        );
        map.insert(
            55,
            Collation {
                name: "utf16_bin",
                charset: "utf16",
                id: 55,
                default: false,
            },
        );
        map.insert(
            101,
            Collation {
                name: "utf16_unicode_ci",
                charset: "utf16",
                id: 101,
                default: false,
            },
        );
        map.insert(
            102,
            Collation {
                name: "utf16_icelandic_ci",
                charset: "utf16",
                id: 102,
                default: false,
            },
        );
        map.insert(
            103,
            Collation {
                name: "utf16_latvian_ci",
                charset: "utf16",
                id: 103,
                default: false,
            },
        );
        map.insert(
            104,
            Collation {
                name: "utf16_romanian_ci",
                charset: "utf16",
                id: 104,
                default: false,
            },
        );
        map.insert(
            105,
            Collation {
                name: "utf16_slovenian_ci",
                charset: "utf16",
                id: 105,
                default: false,
            },
        );
        map.insert(
            106,
            Collation {
                name: "utf16_polish_ci",
                charset: "utf16",
                id: 106,
                default: false,
            },
        );
        map.insert(
            107,
            Collation {
                name: "utf16_estonian_ci",
                charset: "utf16",
                id: 107,
                default: false,
            },
        );
        map.insert(
            108,
            Collation {
                name: "utf16_spanish_ci",
                charset: "utf16",
                id: 108,
                default: false,
            },
        );
        map.insert(
            109,
            Collation {
                name: "utf16_swedish_ci",
                charset: "utf16",
                id: 109,
                default: false,
            },
        );
        map.insert(
            110,
            Collation {
                name: "utf16_turkish_ci",
                charset: "utf16",
                id: 110,
                default: false,
            },
        );
        map.insert(
            111,
            Collation {
                name: "utf16_czech_ci",
                charset: "utf16",
                id: 111,
                default: false,
            },
        );
        map.insert(
            112,
            Collation {
                name: "utf16_danish_ci",
                charset: "utf16",
                id: 112,
                default: false,
            },
        );
        map.insert(
            113,
            Collation {
                name: "utf16_lithuanian_ci",
                charset: "utf16",
                id: 113,
                default: false,
            },
        );
        map.insert(
            114,
            Collation {
                name: "utf16_slovak_ci",
                charset: "utf16",
                id: 114,
                default: false,
            },
        );
        map.insert(
            115,
            Collation {
                name: "utf16_spanish2_ci",
                charset: "utf16",
                id: 115,
                default: false,
            },
        );
        map.insert(
            116,
            Collation {
                name: "utf16_roman_ci",
                charset: "utf16",
                id: 116,
                default: false,
            },
        );
        map.insert(
            117,
            Collation {
                name: "utf16_persian_ci",
                charset: "utf16",
                id: 117,
                default: false,
            },
        );
        map.insert(
            118,
            Collation {
                name: "utf16_esperanto_ci",
                charset: "utf16",
                id: 118,
                default: false,
            },
        );
        map.insert(
            119,
            Collation {
                name: "utf16_hungarian_ci",
                charset: "utf16",
                id: 119,
                default: false,
            },
        );
        map.insert(
            120,
            Collation {
                name: "utf16_sinhala_ci",
                charset: "utf16",
                id: 120,
                default: false,
            },
        );
        map.insert(
            121,
            Collation {
                name: "utf16_german2_ci",
                charset: "utf16",
                id: 121,
                default: false,
            },
        );
        map.insert(
            122,
            Collation {
                name: "utf16_croatian_ci",
                charset: "utf16",
                id: 122,
                default: false,
            },
        );
        map.insert(
            123,
            Collation {
                name: "utf16_unicode_520_ci",
                charset: "utf16",
                id: 123,
                default: false,
            },
        );
        map.insert(
            124,
            Collation {
                name: "utf16_vietnamese_ci",
                charset: "utf16",
                id: 124,
                default: false,
            },
        );
        map.insert(
            56,
            Collation {
                name: "utf16le_general_ci",
                charset: "utf16le",
                id: 56,
                default: true,
            },
        );
        map.insert(
            62,
            Collation {
                name: "utf16le_bin",
                charset: "utf16le",
                id: 62,
                default: false,
            },
        );
        map.insert(
            60,
            Collation {
                name: "utf32_general_ci",
                charset: "utf32",
                id: 60,
                default: true,
            },
        );
        map.insert(
            61,
            Collation {
                name: "utf32_bin",
                charset: "utf32",
                id: 61,
                default: false,
            },
        );
        map.insert(
            160,
            Collation {
                name: "utf32_unicode_ci",
                charset: "utf32",
                id: 160,
                default: false,
            },
        );
        map.insert(
            161,
            Collation {
                name: "utf32_icelandic_ci",
                charset: "utf32",
                id: 161,
                default: false,
            },
        );
        map.insert(
            162,
            Collation {
                name: "utf32_latvian_ci",
                charset: "utf32",
                id: 162,
                default: false,
            },
        );
        map.insert(
            163,
            Collation {
                name: "utf32_romanian_ci",
                charset: "utf32",
                id: 163,
                default: false,
            },
        );
        map.insert(
            164,
            Collation {
                name: "utf32_slovenian_ci",
                charset: "utf32",
                id: 164,
                default: false,
            },
        );
        map.insert(
            165,
            Collation {
                name: "utf32_polish_ci",
                charset: "utf32",
                id: 165,
                default: false,
            },
        );
        map.insert(
            166,
            Collation {
                name: "utf32_estonian_ci",
                charset: "utf32",
                id: 166,
                default: false,
            },
        );
        map.insert(
            167,
            Collation {
                name: "utf32_spanish_ci",
                charset: "utf32",
                id: 167,
                default: false,
            },
        );
        map.insert(
            168,
            Collation {
                name: "utf32_swedish_ci",
                charset: "utf32",
                id: 168,
                default: false,
            },
        );
        map.insert(
            169,
            Collation {
                name: "utf32_turkish_ci",
                charset: "utf32",
                id: 169,
                default: false,
            },
        );
        map.insert(
            170,
            Collation {
                name: "utf32_czech_ci",
                charset: "utf32",
                id: 170,
                default: false,
            },
        );
        map.insert(
            171,
            Collation {
                name: "utf32_danish_ci",
                charset: "utf32",
                id: 171,
                default: false,
            },
        );
        map.insert(
            172,
            Collation {
                name: "utf32_lithuanian_ci",
                charset: "utf32",
                id: 172,
                default: false,
            },
        );
        map.insert(
            173,
            Collation {
                name: "utf32_slovak_ci",
                charset: "utf32",
                id: 173,
                default: false,
            },
        );
        map.insert(
            174,
            Collation {
                name: "utf32_spanish2_ci",
                charset: "utf32",
                id: 174,
                default: false,
            },
        );
        map.insert(
            175,
            Collation {
                name: "utf32_roman_ci",
                charset: "utf32",
                id: 175,
                default: false,
            },
        );
        map.insert(
            176,
            Collation {
                name: "utf32_persian_ci",
                charset: "utf32",
                id: 176,
                default: false,
            },
        );
        map.insert(
            177,
            Collation {
                name: "utf32_esperanto_ci",
                charset: "utf32",
                id: 177,
                default: false,
            },
        );
        map.insert(
            178,
            Collation {
                name: "utf32_hungarian_ci",
                charset: "utf32",
                id: 178,
                default: false,
            },
        );
        map.insert(
            179,
            Collation {
                name: "utf32_sinhala_ci",
                charset: "utf32",
                id: 179,
                default: false,
            },
        );
        map.insert(
            180,
            Collation {
                name: "utf32_german2_ci",
                charset: "utf32",
                id: 180,
                default: false,
            },
        );
        map.insert(
            181,
            Collation {
                name: "utf32_croatian_ci",
                charset: "utf32",
                id: 181,
                default: false,
            },
        );
        map.insert(
            182,
            Collation {
                name: "utf32_unicode_520_ci",
                charset: "utf32",
                id: 182,
                default: false,
            },
        );
        map.insert(
            183,
            Collation {
                name: "utf32_vietnamese_ci",
                charset: "utf32",
                id: 183,
                default: false,
            },
        );
        map.insert(
            33,
            Collation {
                name: "utf8mb3_general_ci",
                charset: "utf8mb3",
                id: 33,
                default: true,
            },
        );
        map.insert(
            76,
            Collation {
                name: "utf8mb3_tolower_ci",
                charset: "utf8mb3",
                id: 76,
                default: false,
            },
        );
        map.insert(
            83,
            Collation {
                name: "utf8mb3_bin",
                charset: "utf8mb3",
                id: 83,
                default: false,
            },
        );
        map.insert(
            192,
            Collation {
                name: "utf8mb3_unicode_ci",
                charset: "utf8mb3",
                id: 192,
                default: false,
            },
        );
        map.insert(
            193,
            Collation {
                name: "utf8mb3_icelandic_ci",
                charset: "utf8mb3",
                id: 193,
                default: false,
            },
        );
        map.insert(
            194,
            Collation {
                name: "utf8mb3_latvian_ci",
                charset: "utf8mb3",
                id: 194,
                default: false,
            },
        );
        map.insert(
            195,
            Collation {
                name: "utf8mb3_romanian_ci",
                charset: "utf8mb3",
                id: 195,
                default: false,
            },
        );
        map.insert(
            196,
            Collation {
                name: "utf8mb3_slovenian_ci",
                charset: "utf8mb3",
                id: 196,
                default: false,
            },
        );
        map.insert(
            197,
            Collation {
                name: "utf8mb3_polish_ci",
                charset: "utf8mb3",
                id: 197,
                default: false,
            },
        );
        map.insert(
            198,
            Collation {
                name: "utf8mb3_estonian_ci",
                charset: "utf8mb3",
                id: 198,
                default: false,
            },
        );
        map.insert(
            199,
            Collation {
                name: "utf8mb3_spanish_ci",
                charset: "utf8mb3",
                id: 199,
                default: false,
            },
        );
        map.insert(
            200,
            Collation {
                name: "utf8mb3_swedish_ci",
                charset: "utf8mb3",
                id: 200,
                default: false,
            },
        );
        map.insert(
            201,
            Collation {
                name: "utf8mb3_turkish_ci",
                charset: "utf8mb3",
                id: 201,
                default: false,
            },
        );
        map.insert(
            202,
            Collation {
                name: "utf8mb3_czech_ci",
                charset: "utf8mb3",
                id: 202,
                default: false,
            },
        );
        map.insert(
            203,
            Collation {
                name: "utf8mb3_danish_ci",
                charset: "utf8mb3",
                id: 203,
                default: false,
            },
        );
        map.insert(
            204,
            Collation {
                name: "utf8mb3_lithuanian_ci",
                charset: "utf8mb3",
                id: 204,
                default: false,
            },
        );
        map.insert(
            205,
            Collation {
                name: "utf8mb3_slovak_ci",
                charset: "utf8mb3",
                id: 205,
                default: false,
            },
        );
        map.insert(
            206,
            Collation {
                name: "utf8mb3_spanish2_ci",
                charset: "utf8mb3",
                id: 206,
                default: false,
            },
        );
        map.insert(
            207,
            Collation {
                name: "utf8mb3_roman_ci",
                charset: "utf8mb3",
                id: 207,
                default: false,
            },
        );
        map.insert(
            208,
            Collation {
                name: "utf8mb3_persian_ci",
                charset: "utf8mb3",
                id: 208,
                default: false,
            },
        );
        map.insert(
            209,
            Collation {
                name: "utf8mb3_esperanto_ci",
                charset: "utf8mb3",
                id: 209,
                default: false,
            },
        );
        map.insert(
            210,
            Collation {
                name: "utf8mb3_hungarian_ci",
                charset: "utf8mb3",
                id: 210,
                default: false,
            },
        );
        map.insert(
            211,
            Collation {
                name: "utf8mb3_sinhala_ci",
                charset: "utf8mb3",
                id: 211,
                default: false,
            },
        );
        map.insert(
            212,
            Collation {
                name: "utf8mb3_german2_ci",
                charset: "utf8mb3",
                id: 212,
                default: false,
            },
        );
        map.insert(
            213,
            Collation {
                name: "utf8mb3_croatian_ci",
                charset: "utf8mb3",
                id: 213,
                default: false,
            },
        );
        map.insert(
            214,
            Collation {
                name: "utf8mb3_unicode_520_ci",
                charset: "utf8mb3",
                id: 214,
                default: false,
            },
        );
        map.insert(
            215,
            Collation {
                name: "utf8mb3_vietnamese_ci",
                charset: "utf8mb3",
                id: 215,
                default: false,
            },
        );
        map.insert(
            223,
            Collation {
                name: "utf8mb3_general_mysql500_ci",
                charset: "utf8mb3",
                id: 223,
                default: false,
            },
        );
        map.insert(
            45,
            Collation {
                name: "utf8mb4_general_ci",
                charset: "utf8mb4",
                id: 45,
                default: false,
            },
        );
        map.insert(
            46,
            Collation {
                name: "utf8mb4_bin",
                charset: "utf8mb4",
                id: 46,
                default: false,
            },
        );
        map.insert(
            224,
            Collation {
                name: "utf8mb4_unicode_ci",
                charset: "utf8mb4",
                id: 224,
                default: false,
            },
        );
        map.insert(
            225,
            Collation {
                name: "utf8mb4_icelandic_ci",
                charset: "utf8mb4",
                id: 225,
                default: false,
            },
        );
        map.insert(
            226,
            Collation {
                name: "utf8mb4_latvian_ci",
                charset: "utf8mb4",
                id: 226,
                default: false,
            },
        );
        map.insert(
            227,
            Collation {
                name: "utf8mb4_romanian_ci",
                charset: "utf8mb4",
                id: 227,
                default: false,
            },
        );
        map.insert(
            228,
            Collation {
                name: "utf8mb4_slovenian_ci",
                charset: "utf8mb4",
                id: 228,
                default: false,
            },
        );
        map.insert(
            229,
            Collation {
                name: "utf8mb4_polish_ci",
                charset: "utf8mb4",
                id: 229,
                default: false,
            },
        );
        map.insert(
            230,
            Collation {
                name: "utf8mb4_estonian_ci",
                charset: "utf8mb4",
                id: 230,
                default: false,
            },
        );
        map.insert(
            231,
            Collation {
                name: "utf8mb4_spanish_ci",
                charset: "utf8mb4",
                id: 231,
                default: false,
            },
        );
        map.insert(
            232,
            Collation {
                name: "utf8mb4_swedish_ci",
                charset: "utf8mb4",
                id: 232,
                default: false,
            },
        );
        map.insert(
            233,
            Collation {
                name: "utf8mb4_turkish_ci",
                charset: "utf8mb4",
                id: 233,
                default: false,
            },
        );
        map.insert(
            234,
            Collation {
                name: "utf8mb4_czech_ci",
                charset: "utf8mb4",
                id: 234,
                default: false,
            },
        );
        map.insert(
            235,
            Collation {
                name: "utf8mb4_danish_ci",
                charset: "utf8mb4",
                id: 235,
                default: false,
            },
        );
        map.insert(
            236,
            Collation {
                name: "utf8mb4_lithuanian_ci",
                charset: "utf8mb4",
                id: 236,
                default: false,
            },
        );
        map.insert(
            237,
            Collation {
                name: "utf8mb4_slovak_ci",
                charset: "utf8mb4",
                id: 237,
                default: false,
            },
        );
        map.insert(
            238,
            Collation {
                name: "utf8mb4_spanish2_ci",
                charset: "utf8mb4",
                id: 238,
                default: false,
            },
        );
        map.insert(
            239,
            Collation {
                name: "utf8mb4_roman_ci",
                charset: "utf8mb4",
                id: 239,
                default: false,
            },
        );
        map.insert(
            240,
            Collation {
                name: "utf8mb4_persian_ci",
                charset: "utf8mb4",
                id: 240,
                default: false,
            },
        );
        map.insert(
            241,
            Collation {
                name: "utf8mb4_esperanto_ci",
                charset: "utf8mb4",
                id: 241,
                default: false,
            },
        );
        map.insert(
            242,
            Collation {
                name: "utf8mb4_hungarian_ci",
                charset: "utf8mb4",
                id: 242,
                default: false,
            },
        );
        map.insert(
            243,
            Collation {
                name: "utf8mb4_sinhala_ci",
                charset: "utf8mb4",
                id: 243,
                default: false,
            },
        );
        map.insert(
            244,
            Collation {
                name: "utf8mb4_german2_ci",
                charset: "utf8mb4",
                id: 244,
                default: false,
            },
        );
        map.insert(
            245,
            Collation {
                name: "utf8mb4_croatian_ci",
                charset: "utf8mb4",
                id: 245,
                default: false,
            },
        );
        map.insert(
            246,
            Collation {
                name: "utf8mb4_unicode_520_ci",
                charset: "utf8mb4",
                id: 246,
                default: false,
            },
        );
        map.insert(
            247,
            Collation {
                name: "utf8mb4_vietnamese_ci",
                charset: "utf8mb4",
                id: 247,
                default: false,
            },
        );
        map.insert(
            255,
            Collation {
                name: "utf8mb4_0900_ai_ci",
                charset: "utf8mb4",
                id: 255,
                default: true,
            },
        );
        map.insert(
            256,
            Collation {
                name: "utf8mb4_de_pb_0900_ai_ci",
                charset: "utf8mb4",
                id: 256,
                default: false,
            },
        );
        map.insert(
            257,
            Collation {
                name: "utf8mb4_is_0900_ai_ci",
                charset: "utf8mb4",
                id: 257,
                default: false,
            },
        );
        map.insert(
            258,
            Collation {
                name: "utf8mb4_lv_0900_ai_ci",
                charset: "utf8mb4",
                id: 258,
                default: false,
            },
        );
        map.insert(
            259,
            Collation {
                name: "utf8mb4_ro_0900_ai_ci",
                charset: "utf8mb4",
                id: 259,
                default: false,
            },
        );
        map.insert(
            260,
            Collation {
                name: "utf8mb4_sl_0900_ai_ci",
                charset: "utf8mb4",
                id: 260,
                default: false,
            },
        );
        map.insert(
            261,
            Collation {
                name: "utf8mb4_pl_0900_ai_ci",
                charset: "utf8mb4",
                id: 261,
                default: false,
            },
        );
        map.insert(
            262,
            Collation {
                name: "utf8mb4_et_0900_ai_ci",
                charset: "utf8mb4",
                id: 262,
                default: false,
            },
        );
        map.insert(
            263,
            Collation {
                name: "utf8mb4_es_0900_ai_ci",
                charset: "utf8mb4",
                id: 263,
                default: false,
            },
        );
        map.insert(
            264,
            Collation {
                name: "utf8mb4_sv_0900_ai_ci",
                charset: "utf8mb4",
                id: 264,
                default: false,
            },
        );
        map.insert(
            265,
            Collation {
                name: "utf8mb4_tr_0900_ai_ci",
                charset: "utf8mb4",
                id: 265,
                default: false,
            },
        );
        map.insert(
            266,
            Collation {
                name: "utf8mb4_cs_0900_ai_ci",
                charset: "utf8mb4",
                id: 266,
                default: false,
            },
        );
        map.insert(
            267,
            Collation {
                name: "utf8mb4_da_0900_ai_ci",
                charset: "utf8mb4",
                id: 267,
                default: false,
            },
        );
        map.insert(
            268,
            Collation {
                name: "utf8mb4_lt_0900_ai_ci",
                charset: "utf8mb4",
                id: 268,
                default: false,
            },
        );
        map.insert(
            269,
            Collation {
                name: "utf8mb4_sk_0900_ai_ci",
                charset: "utf8mb4",
                id: 269,
                default: false,
            },
        );
        map.insert(
            270,
            Collation {
                name: "utf8mb4_es_trad_0900_ai_ci",
                charset: "utf8mb4",
                id: 270,
                default: false,
            },
        );
        map.insert(
            271,
            Collation {
                name: "utf8mb4_la_0900_ai_ci",
                charset: "utf8mb4",
                id: 271,
                default: false,
            },
        );
        map.insert(
            273,
            Collation {
                name: "utf8mb4_eo_0900_ai_ci",
                charset: "utf8mb4",
                id: 273,
                default: false,
            },
        );
        map.insert(
            274,
            Collation {
                name: "utf8mb4_hu_0900_ai_ci",
                charset: "utf8mb4",
                id: 274,
                default: false,
            },
        );
        map.insert(
            275,
            Collation {
                name: "utf8mb4_hr_0900_ai_ci",
                charset: "utf8mb4",
                id: 275,
                default: false,
            },
        );
        map.insert(
            277,
            Collation {
                name: "utf8mb4_vi_0900_ai_ci",
                charset: "utf8mb4",
                id: 277,
                default: false,
            },
        );
        map.insert(
            278,
            Collation {
                name: "utf8mb4_0900_as_cs",
                charset: "utf8mb4",
                id: 278,
                default: false,
            },
        );
        map.insert(
            279,
            Collation {
                name: "utf8mb4_de_pb_0900_as_cs",
                charset: "utf8mb4",
                id: 279,
                default: false,
            },
        );
        map.insert(
            280,
            Collation {
                name: "utf8mb4_is_0900_as_cs",
                charset: "utf8mb4",
                id: 280,
                default: false,
            },
        );
        map.insert(
            281,
            Collation {
                name: "utf8mb4_lv_0900_as_cs",
                charset: "utf8mb4",
                id: 281,
                default: false,
            },
        );
        map.insert(
            282,
            Collation {
                name: "utf8mb4_ro_0900_as_cs",
                charset: "utf8mb4",
                id: 282,
                default: false,
            },
        );
        map.insert(
            283,
            Collation {
                name: "utf8mb4_sl_0900_as_cs",
                charset: "utf8mb4",
                id: 283,
                default: false,
            },
        );
        map.insert(
            284,
            Collation {
                name: "utf8mb4_pl_0900_as_cs",
                charset: "utf8mb4",
                id: 284,
                default: false,
            },
        );
        map.insert(
            285,
            Collation {
                name: "utf8mb4_et_0900_as_cs",
                charset: "utf8mb4",
                id: 285,
                default: false,
            },
        );
        map.insert(
            286,
            Collation {
                name: "utf8mb4_es_0900_as_cs",
                charset: "utf8mb4",
                id: 286,
                default: false,
            },
        );
        map.insert(
            287,
            Collation {
                name: "utf8mb4_sv_0900_as_cs",
                charset: "utf8mb4",
                id: 287,
                default: false,
            },
        );
        map.insert(
            288,
            Collation {
                name: "utf8mb4_tr_0900_as_cs",
                charset: "utf8mb4",
                id: 288,
                default: false,
            },
        );
        map.insert(
            289,
            Collation {
                name: "utf8mb4_cs_0900_as_cs",
                charset: "utf8mb4",
                id: 289,
                default: false,
            },
        );
        map.insert(
            290,
            Collation {
                name: "utf8mb4_da_0900_as_cs",
                charset: "utf8mb4",
                id: 290,
                default: false,
            },
        );
        map.insert(
            291,
            Collation {
                name: "utf8mb4_lt_0900_as_cs",
                charset: "utf8mb4",
                id: 291,
                default: false,
            },
        );
        map.insert(
            292,
            Collation {
                name: "utf8mb4_sk_0900_as_cs",
                charset: "utf8mb4",
                id: 292,
                default: false,
            },
        );
        map.insert(
            293,
            Collation {
                name: "utf8mb4_es_trad_0900_as_cs",
                charset: "utf8mb4",
                id: 293,
                default: false,
            },
        );
        map.insert(
            294,
            Collation {
                name: "utf8mb4_la_0900_as_cs",
                charset: "utf8mb4",
                id: 294,
                default: false,
            },
        );
        map.insert(
            296,
            Collation {
                name: "utf8mb4_eo_0900_as_cs",
                charset: "utf8mb4",
                id: 296,
                default: false,
            },
        );
        map.insert(
            297,
            Collation {
                name: "utf8mb4_hu_0900_as_cs",
                charset: "utf8mb4",
                id: 297,
                default: false,
            },
        );
        map.insert(
            298,
            Collation {
                name: "utf8mb4_hr_0900_as_cs",
                charset: "utf8mb4",
                id: 298,
                default: false,
            },
        );
        map.insert(
            300,
            Collation {
                name: "utf8mb4_vi_0900_as_cs",
                charset: "utf8mb4",
                id: 300,
                default: false,
            },
        );
        map.insert(
            303,
            Collation {
                name: "utf8mb4_ja_0900_as_cs",
                charset: "utf8mb4",
                id: 303,
                default: false,
            },
        );
        map.insert(
            304,
            Collation {
                name: "utf8mb4_ja_0900_as_cs_ks",
                charset: "utf8mb4",
                id: 304,
                default: false,
            },
        );
        map.insert(
            305,
            Collation {
                name: "utf8mb4_0900_as_ci",
                charset: "utf8mb4",
                id: 305,
                default: false,
            },
        );
        map.insert(
            306,
            Collation {
                name: "utf8mb4_ru_0900_ai_ci",
                charset: "utf8mb4",
                id: 306,
                default: false,
            },
        );
        map.insert(
            307,
            Collation {
                name: "utf8mb4_ru_0900_as_cs",
                charset: "utf8mb4",
                id: 307,
                default: false,
            },
        );
        map.insert(
            308,
            Collation {
                name: "utf8mb4_zh_0900_as_cs",
                charset: "utf8mb4",
                id: 308,
                default: false,
            },
        );
        map.insert(
            309,
            Collation {
                name: "utf8mb4_0900_bin",
                charset: "utf8mb4",
                id: 309,
                default: false,
            },
        );
        map.insert(
            310,
            Collation {
                name: "utf8mb4_nb_0900_ai_ci",
                charset: "utf8mb4",
                id: 310,
                default: false,
            },
        );
        map.insert(
            311,
            Collation {
                name: "utf8mb4_nb_0900_as_cs",
                charset: "utf8mb4",
                id: 311,
                default: false,
            },
        );
        map.insert(
            312,
            Collation {
                name: "utf8mb4_nn_0900_ai_ci",
                charset: "utf8mb4",
                id: 312,
                default: false,
            },
        );
        map.insert(
            313,
            Collation {
                name: "utf8mb4_nn_0900_as_cs",
                charset: "utf8mb4",
                id: 313,
                default: false,
            },
        );
        map.insert(
            314,
            Collation {
                name: "utf8mb4_sr_latn_0900_ai_ci",
                charset: "utf8mb4",
                id: 314,
                default: false,
            },
        );
        map.insert(
            315,
            Collation {
                name: "utf8mb4_sr_latn_0900_as_cs",
                charset: "utf8mb4",
                id: 315,
                default: false,
            },
        );
        map.insert(
            316,
            Collation {
                name: "utf8mb4_bs_0900_ai_ci",
                charset: "utf8mb4",
                id: 316,
                default: false,
            },
        );
        map.insert(
            317,
            Collation {
                name: "utf8mb4_bs_0900_as_cs",
                charset: "utf8mb4",
                id: 317,
                default: false,
            },
        );
        map.insert(
            318,
            Collation {
                name: "utf8mb4_bg_0900_ai_ci",
                charset: "utf8mb4",
                id: 318,
                default: false,
            },
        );
        map.insert(
            319,
            Collation {
                name: "utf8mb4_bg_0900_as_cs",
                charset: "utf8mb4",
                id: 319,
                default: false,
            },
        );
        map.insert(
            320,
            Collation {
                name: "utf8mb4_gl_0900_ai_ci",
                charset: "utf8mb4",
                id: 320,
                default: false,
            },
        );
        map.insert(
            321,
            Collation {
                name: "utf8mb4_gl_0900_as_cs",
                charset: "utf8mb4",
                id: 321,
                default: false,
            },
        );
        map.insert(
            322,
            Collation {
                name: "utf8mb4_mn_cyrl_0900_ai_ci",
                charset: "utf8mb4",
                id: 322,
                default: false,
            },
        );
        map.insert(
            323,
            Collation {
                name: "utf8mb4_mn_cyrl_0900_as_cs",
                charset: "utf8mb4",
                id: 323,
                default: false,
            },
        );
        map
    };
}

/// find collation by id
pub fn coll_find(id: u32) -> &'static Collation {
    COLLMAP.get(&id).expect("ERR_COLLATION_NOT_FOUND")
}

#[cfg(test)]
mod meta_consts_tests {



    use log::info;

    use super::*;
    use crate::util;

    #[test]
    fn check_collection_id_consistent() {
        util::init_unit_test();
        for (id, coll) in COLLMAP.iter() {
            info!("{:?}", &coll);
            assert_eq!(*id, coll.id);
        }
    }
}
