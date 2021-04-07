mod utils;

use criterion::{criterion_group, criterion_main};

fn bench_criterion(c: &mut criterion::Criterion) {
    let confs = &[
        utils::Conf {
            group_name: "proximity",
            queries: &[
                "black saint sinner lady ",
                "les dangeureuses 1960 ",
                "The Disneyland Sing-Along Chorus ",
                "Under Great Northern Lights ",
                "7000 Danses Un Jour Dans Notre Vie",
            ],
            criterion: Some(&["proximity"]),
            optional_words: false,
        },
        utils::Conf {
            group_name: "typo",
            queries: &[
                "mongus ",
                "thelonius monk ",
                "Disnaylande ",
                "the white striper ",
                "indochie ",
                "indochien ",
                "klub des loopers ",
                "fear of the duck ",
                "michel depech ",
                "stromal ",
                "dire straights ",
                "Arethla Franklin ",
            ],
            criterion: Some(&["typo"]),
            optional_words: false,
        },
        utils::Conf {
            group_name: "words",
            queries: &[
                "the black saint and the sinner lady and the good doggo ", // four words to pop
                "les liaisons dangeureuses 1793 ", // one word to pop
                "The Disneyland Children's Sing-Alone song ", // two words to pop
                "seven nation mummy ", // one word to pop
                "7000 Danses / Le Baiser / je me trompe de mots ", // four words to pop
                "Bring Your Daughter To The Slaughter but now this is not part of the title ", // nine words to pop
                "whathavenotnsuchforth and then a good amount of words tot pop in order to match the first one ", // 16
            ],
            criterion: Some(&["words"]),
            optional_words: true,
        }
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, bench_criterion);
criterion_main!(benches);