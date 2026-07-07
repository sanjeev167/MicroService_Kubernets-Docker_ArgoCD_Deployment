package com.wallet.ctrl;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/wallet")
@RequiredArgsConstructor
@Slf4j
public class WalletController {

	@GetMapping("")
	 public ResponseEntity<String> get() {
		    
		        // --------------------------------------------------
		        // STEP 3: RESPONSE
		        // --------------------------------------------------
		        return ResponseEntity.ok("Wallet called");

		    
		}

}
