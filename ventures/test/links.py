from ventures_tree import Link, Tree, TreeError

links_ordinary = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("account3", "account4"),
    Link("account4", "account5"),
    Link("account5", "account6"),
]

links_second_tree = [
    Link("R", "D"),
    Link("R", "US"),
    Link("R", "UK"),
    Link("D", "D1"),
    Link("D1", "D1A"),
    Link("D1", "D1B"),
    Link("D1", "D1C"),
    Link("D", "D2"),
    Link("D2", "D2A"),
    Link("D2", "D2B"),
    Link("D", "D3"),
    Link("D3", "D3A"),
    Link("D", "D4"),
    Link("UK", "UK1"),
    Link("UK1", "UK1A"),
    Link("UK1", "UK1B"),
    Link("UK", "UK2"),
    Link("UK2", "UK2A"),
    Link("UK", "UK3"),
    Link("US", "US1"),
    Link("US1", "US1A"),
    Link("US1", "US1B"),
    Link("US", "US2"),
    Link("US2", "US2A"),
    Link("US", "US3"),
]


links_empty_tree = []

links_no_root = [
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("account3", "account4"),
    Link("account4", "account5"),
    Link("account5", "account6")
]

links_two_parents = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("Hellofresh US", "account2")
]

links_invalid_tree = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "Hellofresh US"),
    Link("Hellofresh US", "Hellofresh UK"),

]

links_cyclic_tree = [
    Link("Hellofresh US", "Hellofresh DE"),
    Link("Hellofresh DE", "Hellofresh UK"),
    Link("Hellofresh UK", "Hellofresh DE"),

]