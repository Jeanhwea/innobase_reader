use std::collections::HashMap;
use lazy_static::lazy_static;

#[derive(Debug)]
pub struct Collation {
    pub coll_name: String,
    pub charset_name: String,
    pub id: u32,
    pub is_default: bool,
}

lazy_static! {
    static ref COLLMAP: HashMap<u32, Collation> = {
        let mut map = HashMap::new();
        map.insert(
            32,
            Collation {
                coll_name: "armscii8_general_ci".into(),
                charset_name: "armscii8".into(),
                id: 32,
                is_default: true,
            },
        );
        map.insert(
            64,
            Collation {
                coll_name: "armscii8_bin".into(),
                charset_name: "armscii8".into(),
                id: 64,
                is_default: false,
            },
        );
        map.insert(
            11,
            Collation {
                coll_name: "ascii_general_ci".into(),
                charset_name: "ascii".into(),
                id: 11,
                is_default: true,
            },
        );
        map.insert(
            65,
            Collation {
                coll_name: "ascii_bin".into(),
                charset_name: "ascii".into(),
                id: 65,
                is_default: false,
            },
        );
        map.insert(
            1,
            Collation {
                coll_name: "big5_chinese_ci".into(),
                charset_name: "big5".into(),
                id: 1,
                is_default: true,
            },
        );
        map.insert(
            84,
            Collation {
                coll_name: "big5_bin".into(),
                charset_name: "big5".into(),
                id: 84,
                is_default: false,
            },
        );
        map.insert(
            63,
            Collation {
                coll_name: "binary".into(),
                charset_name: "binary".into(),
                id: 63,
                is_default: true,
            },
        );
        map.insert(
            26,
            Collation {
                coll_name: "cp1250_general_ci".into(),
                charset_name: "cp1250".into(),
                id: 26,
                is_default: true,
            },
        );
        map.insert(
            34,
            Collation {
                coll_name: "cp1250_czech_cs".into(),
                charset_name: "cp1250".into(),
                id: 34,
                is_default: false,
            },
        );
        map.insert(
            44,
            Collation {
                coll_name: "cp1250_croatian_ci".into(),
                charset_name: "cp1250".into(),
                id: 44,
                is_default: false,
            },
        );
        map.insert(
            66,
            Collation {
                coll_name: "cp1250_bin".into(),
                charset_name: "cp1250".into(),
                id: 66,
                is_default: false,
            },
        );
        map.insert(
            99,
            Collation {
                coll_name: "cp1250_polish_ci".into(),
                charset_name: "cp1250".into(),
                id: 99,
                is_default: false,
            },
        );
        map.insert(
            14,
            Collation {
                coll_name: "cp1251_bulgarian_ci".into(),
                charset_name: "cp1251".into(),
                id: 14,
                is_default: false,
            },
        );
        map.insert(
            23,
            Collation {
                coll_name: "cp1251_ukrainian_ci".into(),
                charset_name: "cp1251".into(),
                id: 23,
                is_default: false,
            },
        );
        map.insert(
            50,
            Collation {
                coll_name: "cp1251_bin".into(),
                charset_name: "cp1251".into(),
                id: 50,
                is_default: false,
            },
        );
        map.insert(
            51,
            Collation {
                coll_name: "cp1251_general_ci".into(),
                charset_name: "cp1251".into(),
                id: 51,
                is_default: true,
            },
        );
        map.insert(
            52,
            Collation {
                coll_name: "cp1251_general_cs".into(),
                charset_name: "cp1251".into(),
                id: 52,
                is_default: false,
            },
        );
        map.insert(
            57,
            Collation {
                coll_name: "cp1256_general_ci".into(),
                charset_name: "cp1256".into(),
                id: 57,
                is_default: true,
            },
        );
        map.insert(
            67,
            Collation {
                coll_name: "cp1256_bin".into(),
                charset_name: "cp1256".into(),
                id: 67,
                is_default: false,
            },
        );
        map.insert(
            29,
            Collation {
                coll_name: "cp1257_lithuanian_ci".into(),
                charset_name: "cp1257".into(),
                id: 29,
                is_default: false,
            },
        );
        map.insert(
            58,
            Collation {
                coll_name: "cp1257_bin".into(),
                charset_name: "cp1257".into(),
                id: 58,
                is_default: false,
            },
        );
        map.insert(
            59,
            Collation {
                coll_name: "cp1257_general_ci".into(),
                charset_name: "cp1257".into(),
                id: 59,
                is_default: true,
            },
        );
        map.insert(
            4,
            Collation {
                coll_name: "cp850_general_ci".into(),
                charset_name: "cp850".into(),
                id: 4,
                is_default: true,
            },
        );
        map.insert(
            80,
            Collation {
                coll_name: "cp850_bin".into(),
                charset_name: "cp850".into(),
                id: 80,
                is_default: false,
            },
        );
        map.insert(
            40,
            Collation {
                coll_name: "cp852_general_ci".into(),
                charset_name: "cp852".into(),
                id: 40,
                is_default: true,
            },
        );
        map.insert(
            81,
            Collation {
                coll_name: "cp852_bin".into(),
                charset_name: "cp852".into(),
                id: 81,
                is_default: false,
            },
        );
        map.insert(
            36,
            Collation {
                coll_name: "cp866_general_ci".into(),
                charset_name: "cp866".into(),
                id: 36,
                is_default: true,
            },
        );
        map.insert(
            68,
            Collation {
                coll_name: "cp866_bin".into(),
                charset_name: "cp866".into(),
                id: 68,
                is_default: false,
            },
        );
        map.insert(
            95,
            Collation {
                coll_name: "cp932_japanese_ci".into(),
                charset_name: "cp932".into(),
                id: 95,
                is_default: true,
            },
        );
        map.insert(
            96,
            Collation {
                coll_name: "cp932_bin".into(),
                charset_name: "cp932".into(),
                id: 96,
                is_default: false,
            },
        );
        map.insert(
            3,
            Collation {
                coll_name: "dec8_swedish_ci".into(),
                charset_name: "dec8".into(),
                id: 3,
                is_default: true,
            },
        );
        map.insert(
            69,
            Collation {
                coll_name: "dec8_bin".into(),
                charset_name: "dec8".into(),
                id: 69,
                is_default: false,
            },
        );
        map.insert(
            97,
            Collation {
                coll_name: "eucjpms_japanese_ci".into(),
                charset_name: "eucjpms".into(),
                id: 97,
                is_default: true,
            },
        );
        map.insert(
            98,
            Collation {
                coll_name: "eucjpms_bin".into(),
                charset_name: "eucjpms".into(),
                id: 98,
                is_default: false,
            },
        );
        map.insert(
            19,
            Collation {
                coll_name: "euckr_korean_ci".into(),
                charset_name: "euckr".into(),
                id: 19,
                is_default: true,
            },
        );
        map.insert(
            85,
            Collation {
                coll_name: "euckr_bin".into(),
                charset_name: "euckr".into(),
                id: 85,
                is_default: false,
            },
        );
        map.insert(
            248,
            Collation {
                coll_name: "gb18030_chinese_ci".into(),
                charset_name: "gb18030".into(),
                id: 248,
                is_default: true,
            },
        );
        map.insert(
            249,
            Collation {
                coll_name: "gb18030_bin".into(),
                charset_name: "gb18030".into(),
                id: 249,
                is_default: false,
            },
        );
        map.insert(
            250,
            Collation {
                coll_name: "gb18030_unicode_520_ci".into(),
                charset_name: "gb18030".into(),
                id: 250,
                is_default: false,
            },
        );
        map.insert(
            24,
            Collation {
                coll_name: "gb2312_chinese_ci".into(),
                charset_name: "gb2312".into(),
                id: 24,
                is_default: true,
            },
        );
        map.insert(
            86,
            Collation {
                coll_name: "gb2312_bin".into(),
                charset_name: "gb2312".into(),
                id: 86,
                is_default: false,
            },
        );
        map.insert(
            28,
            Collation {
                coll_name: "gbk_chinese_ci".into(),
                charset_name: "gbk".into(),
                id: 28,
                is_default: true,
            },
        );
        map.insert(
            87,
            Collation {
                coll_name: "gbk_bin".into(),
                charset_name: "gbk".into(),
                id: 87,
                is_default: false,
            },
        );
        map.insert(
            92,
            Collation {
                coll_name: "geostd8_general_ci".into(),
                charset_name: "geostd8".into(),
                id: 92,
                is_default: true,
            },
        );
        map.insert(
            93,
            Collation {
                coll_name: "geostd8_bin".into(),
                charset_name: "geostd8".into(),
                id: 93,
                is_default: false,
            },
        );
        map.insert(
            25,
            Collation {
                coll_name: "greek_general_ci".into(),
                charset_name: "greek".into(),
                id: 25,
                is_default: true,
            },
        );
        map.insert(
            70,
            Collation {
                coll_name: "greek_bin".into(),
                charset_name: "greek".into(),
                id: 70,
                is_default: false,
            },
        );
        map.insert(
            16,
            Collation {
                coll_name: "hebrew_general_ci".into(),
                charset_name: "hebrew".into(),
                id: 16,
                is_default: true,
            },
        );
        map.insert(
            71,
            Collation {
                coll_name: "hebrew_bin".into(),
                charset_name: "hebrew".into(),
                id: 71,
                is_default: false,
            },
        );
        map.insert(
            6,
            Collation {
                coll_name: "hp8_english_ci".into(),
                charset_name: "hp8".into(),
                id: 6,
                is_default: true,
            },
        );
        map.insert(
            72,
            Collation {
                coll_name: "hp8_bin".into(),
                charset_name: "hp8".into(),
                id: 72,
                is_default: false,
            },
        );
        map.insert(
            37,
            Collation {
                coll_name: "keybcs2_general_ci".into(),
                charset_name: "keybcs2".into(),
                id: 37,
                is_default: true,
            },
        );
        map.insert(
            73,
            Collation {
                coll_name: "keybcs2_bin".into(),
                charset_name: "keybcs2".into(),
                id: 73,
                is_default: false,
            },
        );
        map.insert(
            7,
            Collation {
                coll_name: "koi8r_general_ci".into(),
                charset_name: "koi8r".into(),
                id: 7,
                is_default: true,
            },
        );
        map.insert(
            74,
            Collation {
                coll_name: "koi8r_bin".into(),
                charset_name: "koi8r".into(),
                id: 74,
                is_default: false,
            },
        );
        map.insert(
            22,
            Collation {
                coll_name: "koi8u_general_ci".into(),
                charset_name: "koi8u".into(),
                id: 22,
                is_default: true,
            },
        );
        map.insert(
            75,
            Collation {
                coll_name: "koi8u_bin".into(),
                charset_name: "koi8u".into(),
                id: 75,
                is_default: false,
            },
        );
        map.insert(
            5,
            Collation {
                coll_name: "latin1_german1_ci".into(),
                charset_name: "latin1".into(),
                id: 5,
                is_default: false,
            },
        );
        map.insert(
            8,
            Collation {
                coll_name: "latin1_swedish_ci".into(),
                charset_name: "latin1".into(),
                id: 8,
                is_default: true,
            },
        );
        map.insert(
            15,
            Collation {
                coll_name: "latin1_danish_ci".into(),
                charset_name: "latin1".into(),
                id: 15,
                is_default: false,
            },
        );
        map.insert(
            31,
            Collation {
                coll_name: "latin1_german2_ci".into(),
                charset_name: "latin1".into(),
                id: 31,
                is_default: false,
            },
        );
        map.insert(
            47,
            Collation {
                coll_name: "latin1_bin".into(),
                charset_name: "latin1".into(),
                id: 47,
                is_default: false,
            },
        );
        map.insert(
            48,
            Collation {
                coll_name: "latin1_general_ci".into(),
                charset_name: "latin1".into(),
                id: 48,
                is_default: false,
            },
        );
        map.insert(
            49,
            Collation {
                coll_name: "latin1_general_cs".into(),
                charset_name: "latin1".into(),
                id: 49,
                is_default: false,
            },
        );
        map.insert(
            94,
            Collation {
                coll_name: "latin1_spanish_ci".into(),
                charset_name: "latin1".into(),
                id: 94,
                is_default: false,
            },
        );
        map.insert(
            2,
            Collation {
                coll_name: "latin2_czech_cs".into(),
                charset_name: "latin2".into(),
                id: 2,
                is_default: false,
            },
        );
        map.insert(
            9,
            Collation {
                coll_name: "latin2_general_ci".into(),
                charset_name: "latin2".into(),
                id: 9,
                is_default: true,
            },
        );
        map.insert(
            21,
            Collation {
                coll_name: "latin2_hungarian_ci".into(),
                charset_name: "latin2".into(),
                id: 21,
                is_default: false,
            },
        );
        map.insert(
            27,
            Collation {
                coll_name: "latin2_croatian_ci".into(),
                charset_name: "latin2".into(),
                id: 27,
                is_default: false,
            },
        );
        map.insert(
            77,
            Collation {
                coll_name: "latin2_bin".into(),
                charset_name: "latin2".into(),
                id: 77,
                is_default: false,
            },
        );
        map.insert(
            30,
            Collation {
                coll_name: "latin5_turkish_ci".into(),
                charset_name: "latin5".into(),
                id: 30,
                is_default: true,
            },
        );
        map.insert(
            78,
            Collation {
                coll_name: "latin5_bin".into(),
                charset_name: "latin5".into(),
                id: 78,
                is_default: false,
            },
        );
        map.insert(
            20,
            Collation {
                coll_name: "latin7_estonian_cs".into(),
                charset_name: "latin7".into(),
                id: 20,
                is_default: false,
            },
        );
        map.insert(
            41,
            Collation {
                coll_name: "latin7_general_ci".into(),
                charset_name: "latin7".into(),
                id: 41,
                is_default: true,
            },
        );
        map.insert(
            42,
            Collation {
                coll_name: "latin7_general_cs".into(),
                charset_name: "latin7".into(),
                id: 42,
                is_default: false,
            },
        );
        map.insert(
            79,
            Collation {
                coll_name: "latin7_bin".into(),
                charset_name: "latin7".into(),
                id: 79,
                is_default: false,
            },
        );
        map.insert(
            38,
            Collation {
                coll_name: "macce_general_ci".into(),
                charset_name: "macce".into(),
                id: 38,
                is_default: true,
            },
        );
        map.insert(
            43,
            Collation {
                coll_name: "macce_bin".into(),
                charset_name: "macce".into(),
                id: 43,
                is_default: false,
            },
        );
        map.insert(
            39,
            Collation {
                coll_name: "macroman_general_ci".into(),
                charset_name: "macroman".into(),
                id: 39,
                is_default: true,
            },
        );
        map.insert(
            53,
            Collation {
                coll_name: "macroman_bin".into(),
                charset_name: "macroman".into(),
                id: 53,
                is_default: false,
            },
        );
        map.insert(
            13,
            Collation {
                coll_name: "sjis_japanese_ci".into(),
                charset_name: "sjis".into(),
                id: 13,
                is_default: true,
            },
        );
        map.insert(
            88,
            Collation {
                coll_name: "sjis_bin".into(),
                charset_name: "sjis".into(),
                id: 88,
                is_default: false,
            },
        );
        map.insert(
            10,
            Collation {
                coll_name: "swe7_swedish_ci".into(),
                charset_name: "swe7".into(),
                id: 10,
                is_default: true,
            },
        );
        map.insert(
            82,
            Collation {
                coll_name: "swe7_bin".into(),
                charset_name: "swe7".into(),
                id: 82,
                is_default: false,
            },
        );
        map.insert(
            18,
            Collation {
                coll_name: "tis620_thai_ci".into(),
                charset_name: "tis620".into(),
                id: 18,
                is_default: true,
            },
        );
        map.insert(
            89,
            Collation {
                coll_name: "tis620_bin".into(),
                charset_name: "tis620".into(),
                id: 89,
                is_default: false,
            },
        );
        map.insert(
            35,
            Collation {
                coll_name: "ucs2_general_ci".into(),
                charset_name: "ucs2".into(),
                id: 35,
                is_default: true,
            },
        );
        map.insert(
            90,
            Collation {
                coll_name: "ucs2_bin".into(),
                charset_name: "ucs2".into(),
                id: 90,
                is_default: false,
            },
        );
        map.insert(
            128,
            Collation {
                coll_name: "ucs2_unicode_ci".into(),
                charset_name: "ucs2".into(),
                id: 128,
                is_default: false,
            },
        );
        map.insert(
            129,
            Collation {
                coll_name: "ucs2_icelandic_ci".into(),
                charset_name: "ucs2".into(),
                id: 129,
                is_default: false,
            },
        );
        map.insert(
            130,
            Collation {
                coll_name: "ucs2_latvian_ci".into(),
                charset_name: "ucs2".into(),
                id: 130,
                is_default: false,
            },
        );
        map.insert(
            131,
            Collation {
                coll_name: "ucs2_romanian_ci".into(),
                charset_name: "ucs2".into(),
                id: 131,
                is_default: false,
            },
        );
        map.insert(
            132,
            Collation {
                coll_name: "ucs2_slovenian_ci".into(),
                charset_name: "ucs2".into(),
                id: 132,
                is_default: false,
            },
        );
        map.insert(
            133,
            Collation {
                coll_name: "ucs2_polish_ci".into(),
                charset_name: "ucs2".into(),
                id: 133,
                is_default: false,
            },
        );
        map.insert(
            134,
            Collation {
                coll_name: "ucs2_estonian_ci".into(),
                charset_name: "ucs2".into(),
                id: 134,
                is_default: false,
            },
        );
        map.insert(
            135,
            Collation {
                coll_name: "ucs2_spanish_ci".into(),
                charset_name: "ucs2".into(),
                id: 135,
                is_default: false,
            },
        );
        map.insert(
            136,
            Collation {
                coll_name: "ucs2_swedish_ci".into(),
                charset_name: "ucs2".into(),
                id: 136,
                is_default: false,
            },
        );
        map.insert(
            137,
            Collation {
                coll_name: "ucs2_turkish_ci".into(),
                charset_name: "ucs2".into(),
                id: 137,
                is_default: false,
            },
        );
        map.insert(
            138,
            Collation {
                coll_name: "ucs2_czech_ci".into(),
                charset_name: "ucs2".into(),
                id: 138,
                is_default: false,
            },
        );
        map.insert(
            139,
            Collation {
                coll_name: "ucs2_danish_ci".into(),
                charset_name: "ucs2".into(),
                id: 139,
                is_default: false,
            },
        );
        map.insert(
            140,
            Collation {
                coll_name: "ucs2_lithuanian_ci".into(),
                charset_name: "ucs2".into(),
                id: 140,
                is_default: false,
            },
        );
        map.insert(
            141,
            Collation {
                coll_name: "ucs2_slovak_ci".into(),
                charset_name: "ucs2".into(),
                id: 141,
                is_default: false,
            },
        );
        map.insert(
            142,
            Collation {
                coll_name: "ucs2_spanish2_ci".into(),
                charset_name: "ucs2".into(),
                id: 142,
                is_default: false,
            },
        );
        map.insert(
            143,
            Collation {
                coll_name: "ucs2_roman_ci".into(),
                charset_name: "ucs2".into(),
                id: 143,
                is_default: false,
            },
        );
        map.insert(
            144,
            Collation {
                coll_name: "ucs2_persian_ci".into(),
                charset_name: "ucs2".into(),
                id: 144,
                is_default: false,
            },
        );
        map.insert(
            145,
            Collation {
                coll_name: "ucs2_esperanto_ci".into(),
                charset_name: "ucs2".into(),
                id: 145,
                is_default: false,
            },
        );
        map.insert(
            146,
            Collation {
                coll_name: "ucs2_hungarian_ci".into(),
                charset_name: "ucs2".into(),
                id: 146,
                is_default: false,
            },
        );
        map.insert(
            147,
            Collation {
                coll_name: "ucs2_sinhala_ci".into(),
                charset_name: "ucs2".into(),
                id: 147,
                is_default: false,
            },
        );
        map.insert(
            148,
            Collation {
                coll_name: "ucs2_german2_ci".into(),
                charset_name: "ucs2".into(),
                id: 148,
                is_default: false,
            },
        );
        map.insert(
            149,
            Collation {
                coll_name: "ucs2_croatian_ci".into(),
                charset_name: "ucs2".into(),
                id: 149,
                is_default: false,
            },
        );
        map.insert(
            150,
            Collation {
                coll_name: "ucs2_unicode_520_ci".into(),
                charset_name: "ucs2".into(),
                id: 150,
                is_default: false,
            },
        );
        map.insert(
            151,
            Collation {
                coll_name: "ucs2_vietnamese_ci".into(),
                charset_name: "ucs2".into(),
                id: 151,
                is_default: false,
            },
        );
        map.insert(
            159,
            Collation {
                coll_name: "ucs2_general_mysql500_ci".into(),
                charset_name: "ucs2".into(),
                id: 159,
                is_default: false,
            },
        );
        map.insert(
            12,
            Collation {
                coll_name: "ujis_japanese_ci".into(),
                charset_name: "ujis".into(),
                id: 12,
                is_default: true,
            },
        );
        map.insert(
            91,
            Collation {
                coll_name: "ujis_bin".into(),
                charset_name: "ujis".into(),
                id: 91,
                is_default: false,
            },
        );
        map.insert(
            54,
            Collation {
                coll_name: "utf16_general_ci".into(),
                charset_name: "utf16".into(),
                id: 54,
                is_default: true,
            },
        );
        map.insert(
            55,
            Collation {
                coll_name: "utf16_bin".into(),
                charset_name: "utf16".into(),
                id: 55,
                is_default: false,
            },
        );
        map.insert(
            101,
            Collation {
                coll_name: "utf16_unicode_ci".into(),
                charset_name: "utf16".into(),
                id: 101,
                is_default: false,
            },
        );
        map.insert(
            102,
            Collation {
                coll_name: "utf16_icelandic_ci".into(),
                charset_name: "utf16".into(),
                id: 102,
                is_default: false,
            },
        );
        map.insert(
            103,
            Collation {
                coll_name: "utf16_latvian_ci".into(),
                charset_name: "utf16".into(),
                id: 103,
                is_default: false,
            },
        );
        map.insert(
            104,
            Collation {
                coll_name: "utf16_romanian_ci".into(),
                charset_name: "utf16".into(),
                id: 104,
                is_default: false,
            },
        );
        map.insert(
            105,
            Collation {
                coll_name: "utf16_slovenian_ci".into(),
                charset_name: "utf16".into(),
                id: 105,
                is_default: false,
            },
        );
        map.insert(
            106,
            Collation {
                coll_name: "utf16_polish_ci".into(),
                charset_name: "utf16".into(),
                id: 106,
                is_default: false,
            },
        );
        map.insert(
            107,
            Collation {
                coll_name: "utf16_estonian_ci".into(),
                charset_name: "utf16".into(),
                id: 107,
                is_default: false,
            },
        );
        map.insert(
            108,
            Collation {
                coll_name: "utf16_spanish_ci".into(),
                charset_name: "utf16".into(),
                id: 108,
                is_default: false,
            },
        );
        map.insert(
            109,
            Collation {
                coll_name: "utf16_swedish_ci".into(),
                charset_name: "utf16".into(),
                id: 109,
                is_default: false,
            },
        );
        map.insert(
            110,
            Collation {
                coll_name: "utf16_turkish_ci".into(),
                charset_name: "utf16".into(),
                id: 110,
                is_default: false,
            },
        );
        map.insert(
            111,
            Collation {
                coll_name: "utf16_czech_ci".into(),
                charset_name: "utf16".into(),
                id: 111,
                is_default: false,
            },
        );
        map.insert(
            112,
            Collation {
                coll_name: "utf16_danish_ci".into(),
                charset_name: "utf16".into(),
                id: 112,
                is_default: false,
            },
        );
        map.insert(
            113,
            Collation {
                coll_name: "utf16_lithuanian_ci".into(),
                charset_name: "utf16".into(),
                id: 113,
                is_default: false,
            },
        );
        map.insert(
            114,
            Collation {
                coll_name: "utf16_slovak_ci".into(),
                charset_name: "utf16".into(),
                id: 114,
                is_default: false,
            },
        );
        map.insert(
            115,
            Collation {
                coll_name: "utf16_spanish2_ci".into(),
                charset_name: "utf16".into(),
                id: 115,
                is_default: false,
            },
        );
        map.insert(
            116,
            Collation {
                coll_name: "utf16_roman_ci".into(),
                charset_name: "utf16".into(),
                id: 116,
                is_default: false,
            },
        );
        map.insert(
            117,
            Collation {
                coll_name: "utf16_persian_ci".into(),
                charset_name: "utf16".into(),
                id: 117,
                is_default: false,
            },
        );
        map.insert(
            118,
            Collation {
                coll_name: "utf16_esperanto_ci".into(),
                charset_name: "utf16".into(),
                id: 118,
                is_default: false,
            },
        );
        map.insert(
            119,
            Collation {
                coll_name: "utf16_hungarian_ci".into(),
                charset_name: "utf16".into(),
                id: 119,
                is_default: false,
            },
        );
        map.insert(
            120,
            Collation {
                coll_name: "utf16_sinhala_ci".into(),
                charset_name: "utf16".into(),
                id: 120,
                is_default: false,
            },
        );
        map.insert(
            121,
            Collation {
                coll_name: "utf16_german2_ci".into(),
                charset_name: "utf16".into(),
                id: 121,
                is_default: false,
            },
        );
        map.insert(
            122,
            Collation {
                coll_name: "utf16_croatian_ci".into(),
                charset_name: "utf16".into(),
                id: 122,
                is_default: false,
            },
        );
        map.insert(
            123,
            Collation {
                coll_name: "utf16_unicode_520_ci".into(),
                charset_name: "utf16".into(),
                id: 123,
                is_default: false,
            },
        );
        map.insert(
            124,
            Collation {
                coll_name: "utf16_vietnamese_ci".into(),
                charset_name: "utf16".into(),
                id: 124,
                is_default: false,
            },
        );
        map.insert(
            56,
            Collation {
                coll_name: "utf16le_general_ci".into(),
                charset_name: "utf16le".into(),
                id: 56,
                is_default: true,
            },
        );
        map.insert(
            62,
            Collation {
                coll_name: "utf16le_bin".into(),
                charset_name: "utf16le".into(),
                id: 62,
                is_default: false,
            },
        );
        map.insert(
            60,
            Collation {
                coll_name: "utf32_general_ci".into(),
                charset_name: "utf32".into(),
                id: 60,
                is_default: true,
            },
        );
        map.insert(
            61,
            Collation {
                coll_name: "utf32_bin".into(),
                charset_name: "utf32".into(),
                id: 61,
                is_default: false,
            },
        );
        map.insert(
            160,
            Collation {
                coll_name: "utf32_unicode_ci".into(),
                charset_name: "utf32".into(),
                id: 160,
                is_default: false,
            },
        );
        map.insert(
            161,
            Collation {
                coll_name: "utf32_icelandic_ci".into(),
                charset_name: "utf32".into(),
                id: 161,
                is_default: false,
            },
        );
        map.insert(
            162,
            Collation {
                coll_name: "utf32_latvian_ci".into(),
                charset_name: "utf32".into(),
                id: 162,
                is_default: false,
            },
        );
        map.insert(
            163,
            Collation {
                coll_name: "utf32_romanian_ci".into(),
                charset_name: "utf32".into(),
                id: 163,
                is_default: false,
            },
        );
        map.insert(
            164,
            Collation {
                coll_name: "utf32_slovenian_ci".into(),
                charset_name: "utf32".into(),
                id: 164,
                is_default: false,
            },
        );
        map.insert(
            165,
            Collation {
                coll_name: "utf32_polish_ci".into(),
                charset_name: "utf32".into(),
                id: 165,
                is_default: false,
            },
        );
        map.insert(
            166,
            Collation {
                coll_name: "utf32_estonian_ci".into(),
                charset_name: "utf32".into(),
                id: 166,
                is_default: false,
            },
        );
        map.insert(
            167,
            Collation {
                coll_name: "utf32_spanish_ci".into(),
                charset_name: "utf32".into(),
                id: 167,
                is_default: false,
            },
        );
        map.insert(
            168,
            Collation {
                coll_name: "utf32_swedish_ci".into(),
                charset_name: "utf32".into(),
                id: 168,
                is_default: false,
            },
        );
        map.insert(
            169,
            Collation {
                coll_name: "utf32_turkish_ci".into(),
                charset_name: "utf32".into(),
                id: 169,
                is_default: false,
            },
        );
        map.insert(
            170,
            Collation {
                coll_name: "utf32_czech_ci".into(),
                charset_name: "utf32".into(),
                id: 170,
                is_default: false,
            },
        );
        map.insert(
            171,
            Collation {
                coll_name: "utf32_danish_ci".into(),
                charset_name: "utf32".into(),
                id: 171,
                is_default: false,
            },
        );
        map.insert(
            172,
            Collation {
                coll_name: "utf32_lithuanian_ci".into(),
                charset_name: "utf32".into(),
                id: 172,
                is_default: false,
            },
        );
        map.insert(
            173,
            Collation {
                coll_name: "utf32_slovak_ci".into(),
                charset_name: "utf32".into(),
                id: 173,
                is_default: false,
            },
        );
        map.insert(
            174,
            Collation {
                coll_name: "utf32_spanish2_ci".into(),
                charset_name: "utf32".into(),
                id: 174,
                is_default: false,
            },
        );
        map.insert(
            175,
            Collation {
                coll_name: "utf32_roman_ci".into(),
                charset_name: "utf32".into(),
                id: 175,
                is_default: false,
            },
        );
        map.insert(
            176,
            Collation {
                coll_name: "utf32_persian_ci".into(),
                charset_name: "utf32".into(),
                id: 176,
                is_default: false,
            },
        );
        map.insert(
            177,
            Collation {
                coll_name: "utf32_esperanto_ci".into(),
                charset_name: "utf32".into(),
                id: 177,
                is_default: false,
            },
        );
        map.insert(
            178,
            Collation {
                coll_name: "utf32_hungarian_ci".into(),
                charset_name: "utf32".into(),
                id: 178,
                is_default: false,
            },
        );
        map.insert(
            179,
            Collation {
                coll_name: "utf32_sinhala_ci".into(),
                charset_name: "utf32".into(),
                id: 179,
                is_default: false,
            },
        );
        map.insert(
            180,
            Collation {
                coll_name: "utf32_german2_ci".into(),
                charset_name: "utf32".into(),
                id: 180,
                is_default: false,
            },
        );
        map.insert(
            181,
            Collation {
                coll_name: "utf32_croatian_ci".into(),
                charset_name: "utf32".into(),
                id: 181,
                is_default: false,
            },
        );
        map.insert(
            182,
            Collation {
                coll_name: "utf32_unicode_520_ci".into(),
                charset_name: "utf32".into(),
                id: 182,
                is_default: false,
            },
        );
        map.insert(
            183,
            Collation {
                coll_name: "utf32_vietnamese_ci".into(),
                charset_name: "utf32".into(),
                id: 183,
                is_default: false,
            },
        );
        map.insert(
            33,
            Collation {
                coll_name: "utf8mb3_general_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 33,
                is_default: true,
            },
        );
        map.insert(
            76,
            Collation {
                coll_name: "utf8mb3_tolower_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 76,
                is_default: false,
            },
        );
        map.insert(
            83,
            Collation {
                coll_name: "utf8mb3_bin".into(),
                charset_name: "utf8mb3".into(),
                id: 83,
                is_default: false,
            },
        );
        map.insert(
            192,
            Collation {
                coll_name: "utf8mb3_unicode_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 192,
                is_default: false,
            },
        );
        map.insert(
            193,
            Collation {
                coll_name: "utf8mb3_icelandic_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 193,
                is_default: false,
            },
        );
        map.insert(
            194,
            Collation {
                coll_name: "utf8mb3_latvian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 194,
                is_default: false,
            },
        );
        map.insert(
            195,
            Collation {
                coll_name: "utf8mb3_romanian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 195,
                is_default: false,
            },
        );
        map.insert(
            196,
            Collation {
                coll_name: "utf8mb3_slovenian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 196,
                is_default: false,
            },
        );
        map.insert(
            197,
            Collation {
                coll_name: "utf8mb3_polish_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 197,
                is_default: false,
            },
        );
        map.insert(
            198,
            Collation {
                coll_name: "utf8mb3_estonian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 198,
                is_default: false,
            },
        );
        map.insert(
            199,
            Collation {
                coll_name: "utf8mb3_spanish_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 199,
                is_default: false,
            },
        );
        map.insert(
            200,
            Collation {
                coll_name: "utf8mb3_swedish_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 200,
                is_default: false,
            },
        );
        map.insert(
            201,
            Collation {
                coll_name: "utf8mb3_turkish_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 201,
                is_default: false,
            },
        );
        map.insert(
            202,
            Collation {
                coll_name: "utf8mb3_czech_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 202,
                is_default: false,
            },
        );
        map.insert(
            203,
            Collation {
                coll_name: "utf8mb3_danish_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 203,
                is_default: false,
            },
        );
        map.insert(
            204,
            Collation {
                coll_name: "utf8mb3_lithuanian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 204,
                is_default: false,
            },
        );
        map.insert(
            205,
            Collation {
                coll_name: "utf8mb3_slovak_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 205,
                is_default: false,
            },
        );
        map.insert(
            206,
            Collation {
                coll_name: "utf8mb3_spanish2_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 206,
                is_default: false,
            },
        );
        map.insert(
            207,
            Collation {
                coll_name: "utf8mb3_roman_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 207,
                is_default: false,
            },
        );
        map.insert(
            208,
            Collation {
                coll_name: "utf8mb3_persian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 208,
                is_default: false,
            },
        );
        map.insert(
            209,
            Collation {
                coll_name: "utf8mb3_esperanto_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 209,
                is_default: false,
            },
        );
        map.insert(
            210,
            Collation {
                coll_name: "utf8mb3_hungarian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 210,
                is_default: false,
            },
        );
        map.insert(
            211,
            Collation {
                coll_name: "utf8mb3_sinhala_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 211,
                is_default: false,
            },
        );
        map.insert(
            212,
            Collation {
                coll_name: "utf8mb3_german2_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 212,
                is_default: false,
            },
        );
        map.insert(
            213,
            Collation {
                coll_name: "utf8mb3_croatian_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 213,
                is_default: false,
            },
        );
        map.insert(
            214,
            Collation {
                coll_name: "utf8mb3_unicode_520_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 214,
                is_default: false,
            },
        );
        map.insert(
            215,
            Collation {
                coll_name: "utf8mb3_vietnamese_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 215,
                is_default: false,
            },
        );
        map.insert(
            223,
            Collation {
                coll_name: "utf8mb3_general_mysql500_ci".into(),
                charset_name: "utf8mb3".into(),
                id: 223,
                is_default: false,
            },
        );
        map.insert(
            45,
            Collation {
                coll_name: "utf8mb4_general_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 45,
                is_default: false,
            },
        );
        map.insert(
            46,
            Collation {
                coll_name: "utf8mb4_bin".into(),
                charset_name: "utf8mb4".into(),
                id: 46,
                is_default: false,
            },
        );
        map.insert(
            224,
            Collation {
                coll_name: "utf8mb4_unicode_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 224,
                is_default: false,
            },
        );
        map.insert(
            225,
            Collation {
                coll_name: "utf8mb4_icelandic_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 225,
                is_default: false,
            },
        );
        map.insert(
            226,
            Collation {
                coll_name: "utf8mb4_latvian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 226,
                is_default: false,
            },
        );
        map.insert(
            227,
            Collation {
                coll_name: "utf8mb4_romanian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 227,
                is_default: false,
            },
        );
        map.insert(
            228,
            Collation {
                coll_name: "utf8mb4_slovenian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 228,
                is_default: false,
            },
        );
        map.insert(
            229,
            Collation {
                coll_name: "utf8mb4_polish_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 229,
                is_default: false,
            },
        );
        map.insert(
            230,
            Collation {
                coll_name: "utf8mb4_estonian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 230,
                is_default: false,
            },
        );
        map.insert(
            231,
            Collation {
                coll_name: "utf8mb4_spanish_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 231,
                is_default: false,
            },
        );
        map.insert(
            232,
            Collation {
                coll_name: "utf8mb4_swedish_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 232,
                is_default: false,
            },
        );
        map.insert(
            233,
            Collation {
                coll_name: "utf8mb4_turkish_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 233,
                is_default: false,
            },
        );
        map.insert(
            234,
            Collation {
                coll_name: "utf8mb4_czech_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 234,
                is_default: false,
            },
        );
        map.insert(
            235,
            Collation {
                coll_name: "utf8mb4_danish_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 235,
                is_default: false,
            },
        );
        map.insert(
            236,
            Collation {
                coll_name: "utf8mb4_lithuanian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 236,
                is_default: false,
            },
        );
        map.insert(
            237,
            Collation {
                coll_name: "utf8mb4_slovak_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 237,
                is_default: false,
            },
        );
        map.insert(
            238,
            Collation {
                coll_name: "utf8mb4_spanish2_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 238,
                is_default: false,
            },
        );
        map.insert(
            239,
            Collation {
                coll_name: "utf8mb4_roman_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 239,
                is_default: false,
            },
        );
        map.insert(
            240,
            Collation {
                coll_name: "utf8mb4_persian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 240,
                is_default: false,
            },
        );
        map.insert(
            241,
            Collation {
                coll_name: "utf8mb4_esperanto_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 241,
                is_default: false,
            },
        );
        map.insert(
            242,
            Collation {
                coll_name: "utf8mb4_hungarian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 242,
                is_default: false,
            },
        );
        map.insert(
            243,
            Collation {
                coll_name: "utf8mb4_sinhala_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 243,
                is_default: false,
            },
        );
        map.insert(
            244,
            Collation {
                coll_name: "utf8mb4_german2_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 244,
                is_default: false,
            },
        );
        map.insert(
            245,
            Collation {
                coll_name: "utf8mb4_croatian_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 245,
                is_default: false,
            },
        );
        map.insert(
            246,
            Collation {
                coll_name: "utf8mb4_unicode_520_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 246,
                is_default: false,
            },
        );
        map.insert(
            247,
            Collation {
                coll_name: "utf8mb4_vietnamese_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 247,
                is_default: false,
            },
        );
        map.insert(
            255,
            Collation {
                coll_name: "utf8mb4_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 255,
                is_default: true,
            },
        );
        map.insert(
            256,
            Collation {
                coll_name: "utf8mb4_de_pb_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 256,
                is_default: false,
            },
        );
        map.insert(
            257,
            Collation {
                coll_name: "utf8mb4_is_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 257,
                is_default: false,
            },
        );
        map.insert(
            258,
            Collation {
                coll_name: "utf8mb4_lv_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 258,
                is_default: false,
            },
        );
        map.insert(
            259,
            Collation {
                coll_name: "utf8mb4_ro_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 259,
                is_default: false,
            },
        );
        map.insert(
            260,
            Collation {
                coll_name: "utf8mb4_sl_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 260,
                is_default: false,
            },
        );
        map.insert(
            261,
            Collation {
                coll_name: "utf8mb4_pl_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 261,
                is_default: false,
            },
        );
        map.insert(
            262,
            Collation {
                coll_name: "utf8mb4_et_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 262,
                is_default: false,
            },
        );
        map.insert(
            263,
            Collation {
                coll_name: "utf8mb4_es_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 263,
                is_default: false,
            },
        );
        map.insert(
            264,
            Collation {
                coll_name: "utf8mb4_sv_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 264,
                is_default: false,
            },
        );
        map.insert(
            265,
            Collation {
                coll_name: "utf8mb4_tr_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 265,
                is_default: false,
            },
        );
        map.insert(
            266,
            Collation {
                coll_name: "utf8mb4_cs_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 266,
                is_default: false,
            },
        );
        map.insert(
            267,
            Collation {
                coll_name: "utf8mb4_da_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 267,
                is_default: false,
            },
        );
        map.insert(
            268,
            Collation {
                coll_name: "utf8mb4_lt_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 268,
                is_default: false,
            },
        );
        map.insert(
            269,
            Collation {
                coll_name: "utf8mb4_sk_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 269,
                is_default: false,
            },
        );
        map.insert(
            270,
            Collation {
                coll_name: "utf8mb4_es_trad_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 270,
                is_default: false,
            },
        );
        map.insert(
            271,
            Collation {
                coll_name: "utf8mb4_la_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 271,
                is_default: false,
            },
        );
        map.insert(
            273,
            Collation {
                coll_name: "utf8mb4_eo_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 273,
                is_default: false,
            },
        );
        map.insert(
            274,
            Collation {
                coll_name: "utf8mb4_hu_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 274,
                is_default: false,
            },
        );
        map.insert(
            275,
            Collation {
                coll_name: "utf8mb4_hr_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 275,
                is_default: false,
            },
        );
        map.insert(
            277,
            Collation {
                coll_name: "utf8mb4_vi_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 277,
                is_default: false,
            },
        );
        map.insert(
            278,
            Collation {
                coll_name: "utf8mb4_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 278,
                is_default: false,
            },
        );
        map.insert(
            279,
            Collation {
                coll_name: "utf8mb4_de_pb_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 279,
                is_default: false,
            },
        );
        map.insert(
            280,
            Collation {
                coll_name: "utf8mb4_is_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 280,
                is_default: false,
            },
        );
        map.insert(
            281,
            Collation {
                coll_name: "utf8mb4_lv_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 281,
                is_default: false,
            },
        );
        map.insert(
            282,
            Collation {
                coll_name: "utf8mb4_ro_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 282,
                is_default: false,
            },
        );
        map.insert(
            283,
            Collation {
                coll_name: "utf8mb4_sl_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 283,
                is_default: false,
            },
        );
        map.insert(
            284,
            Collation {
                coll_name: "utf8mb4_pl_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 284,
                is_default: false,
            },
        );
        map.insert(
            285,
            Collation {
                coll_name: "utf8mb4_et_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 285,
                is_default: false,
            },
        );
        map.insert(
            286,
            Collation {
                coll_name: "utf8mb4_es_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 286,
                is_default: false,
            },
        );
        map.insert(
            287,
            Collation {
                coll_name: "utf8mb4_sv_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 287,
                is_default: false,
            },
        );
        map.insert(
            288,
            Collation {
                coll_name: "utf8mb4_tr_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 288,
                is_default: false,
            },
        );
        map.insert(
            289,
            Collation {
                coll_name: "utf8mb4_cs_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 289,
                is_default: false,
            },
        );
        map.insert(
            290,
            Collation {
                coll_name: "utf8mb4_da_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 290,
                is_default: false,
            },
        );
        map.insert(
            291,
            Collation {
                coll_name: "utf8mb4_lt_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 291,
                is_default: false,
            },
        );
        map.insert(
            292,
            Collation {
                coll_name: "utf8mb4_sk_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 292,
                is_default: false,
            },
        );
        map.insert(
            293,
            Collation {
                coll_name: "utf8mb4_es_trad_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 293,
                is_default: false,
            },
        );
        map.insert(
            294,
            Collation {
                coll_name: "utf8mb4_la_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 294,
                is_default: false,
            },
        );
        map.insert(
            296,
            Collation {
                coll_name: "utf8mb4_eo_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 296,
                is_default: false,
            },
        );
        map.insert(
            297,
            Collation {
                coll_name: "utf8mb4_hu_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 297,
                is_default: false,
            },
        );
        map.insert(
            298,
            Collation {
                coll_name: "utf8mb4_hr_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 298,
                is_default: false,
            },
        );
        map.insert(
            300,
            Collation {
                coll_name: "utf8mb4_vi_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 300,
                is_default: false,
            },
        );
        map.insert(
            303,
            Collation {
                coll_name: "utf8mb4_ja_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 303,
                is_default: false,
            },
        );
        map.insert(
            304,
            Collation {
                coll_name: "utf8mb4_ja_0900_as_cs_ks".into(),
                charset_name: "utf8mb4".into(),
                id: 304,
                is_default: false,
            },
        );
        map.insert(
            305,
            Collation {
                coll_name: "utf8mb4_0900_as_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 305,
                is_default: false,
            },
        );
        map.insert(
            306,
            Collation {
                coll_name: "utf8mb4_ru_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 306,
                is_default: false,
            },
        );
        map.insert(
            307,
            Collation {
                coll_name: "utf8mb4_ru_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 307,
                is_default: false,
            },
        );
        map.insert(
            308,
            Collation {
                coll_name: "utf8mb4_zh_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 308,
                is_default: false,
            },
        );
        map.insert(
            309,
            Collation {
                coll_name: "utf8mb4_0900_bin".into(),
                charset_name: "utf8mb4".into(),
                id: 309,
                is_default: false,
            },
        );
        map.insert(
            310,
            Collation {
                coll_name: "utf8mb4_nb_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 310,
                is_default: false,
            },
        );
        map.insert(
            311,
            Collation {
                coll_name: "utf8mb4_nb_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 311,
                is_default: false,
            },
        );
        map.insert(
            312,
            Collation {
                coll_name: "utf8mb4_nn_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 312,
                is_default: false,
            },
        );
        map.insert(
            313,
            Collation {
                coll_name: "utf8mb4_nn_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 313,
                is_default: false,
            },
        );
        map.insert(
            314,
            Collation {
                coll_name: "utf8mb4_sr_latn_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 314,
                is_default: false,
            },
        );
        map.insert(
            315,
            Collation {
                coll_name: "utf8mb4_sr_latn_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 315,
                is_default: false,
            },
        );
        map.insert(
            316,
            Collation {
                coll_name: "utf8mb4_bs_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 316,
                is_default: false,
            },
        );
        map.insert(
            317,
            Collation {
                coll_name: "utf8mb4_bs_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 317,
                is_default: false,
            },
        );
        map.insert(
            318,
            Collation {
                coll_name: "utf8mb4_bg_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 318,
                is_default: false,
            },
        );
        map.insert(
            319,
            Collation {
                coll_name: "utf8mb4_bg_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 319,
                is_default: false,
            },
        );
        map.insert(
            320,
            Collation {
                coll_name: "utf8mb4_gl_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 320,
                is_default: false,
            },
        );
        map.insert(
            321,
            Collation {
                coll_name: "utf8mb4_gl_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 321,
                is_default: false,
            },
        );
        map.insert(
            322,
            Collation {
                coll_name: "utf8mb4_mn_cyrl_0900_ai_ci".into(),
                charset_name: "utf8mb4".into(),
                id: 322,
                is_default: false,
            },
        );
        map.insert(
            323,
            Collation {
                coll_name: "utf8mb4_mn_cyrl_0900_as_cs".into(),
                charset_name: "utf8mb4".into(),
                id: 323,
                is_default: false,
            },
        );
        map
    };
}

pub fn get_collation(id: u32) -> &'static Collation {
    COLLMAP.get(&id).unwrap()
}

#[cfg(test)]
mod meta_consts_tests {

    use std::env::set_var;
    use log::info;
    use crate::util;

    use super::*;

    fn setup() {
        set_var("RUST_LOG", "info");
        util::init();
    }

    #[test]
    fn test_get_collection() {
        setup();
        let coll = get_collation(255);
        assert!(coll.id > 0);
        info!("{:?}", coll);
    }
}
