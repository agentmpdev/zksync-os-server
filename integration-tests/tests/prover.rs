#![cfg(feature = "prover-tests")]

use zksync_os_integration_tests::integration_test_matrix;

integration_test_matrix!(
    #[test_log::test(tokio::test)]
    prover,
    |case| async move {
        let tester = case.builder().enable_prover().build().await?;
        tester.prover_tester.wait_for_batch_proven(1).await?;
        Ok(())
    }
);
