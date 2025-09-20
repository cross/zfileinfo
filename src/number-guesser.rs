use std::cmp::Ordering;
use std::io;

use rand::Rng;

use std::num::IntErrorKind;

fn main() {
    println!("Guess the number!");

    let secret_number = rand::rng().random_range(1..=100);

    println!("The secret number is {secret_number}");

    loop {
	println!("Please input your guess.");

	let mut guess = String::new();

	io::stdin()
	    .read_line(&mut guess)
	    .expect("Failed to read line");

	let guess: u32 = match guess.trim().parse() {
	    Ok(num) => num,
	    Err(e) => match e.kind() {
		IntErrorKind::Empty => break,
		IntErrorKind::InvalidDigit => {
			if guess.trim().parse::<i32>().is_ok() {
				println!("Unsupported: please enter a positive number.");
			} else if guess.trim().parse::<f32>().is_ok() {
				println!("Unsupported: Oops, only intgers are supported!");
			} else {
				println!("Err: {e} {}. Please type an integer.", guess.trim());
			}
		    continue;
		},
		_ => {
		    println!("{:#?}", e.kind());
		    println!("Err: {e}. Please type an integer.");
		    continue;
		}
	    }
	};

	println!("You guessed: {guess}");

	match guess.cmp(&secret_number) {
	    Ordering::Less => println!("Too small!"),
	    Ordering::Greater => println!("Too big!"),
	    Ordering::Equal => {
		println!("You win!");
		break;
	    }
	}
    }

}
