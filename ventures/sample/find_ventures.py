from ventures_tree import Link, find_venture

links = [
    Link("Root", "Hellofresh UK"),
    Link("Root", "Hellofresh US"),
    Link("Hellofresh UK", "account1"),
    Link("Hellofresh UK", "account2"),
    Link("Hellofresh US", "account3"),
    Link("account3", "account4"),
    Link("account4", "account5"),
    Link("account5", "account6"),
]



if __name__ == "__main__":
    print(find_venture(links, "account5"))
    print(find_venture(links, "account2"))