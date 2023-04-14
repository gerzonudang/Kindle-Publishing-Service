package com.tct.mastery.task1;

import com.amazon.ata.advertising.service.activity.dagger.GenerateAdActivityDagger;
import com.amazon.ata.advertising.service.model.requests.GenerateAdvertisementRequest;
import com.amazon.ata.advertising.service.model.responses.GenerateAdvertisementResponse;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static com.tct.helper.TestConstants.*;
import static org.testng.Assert.*;

public class MasteryTaskOneLogicTests {

    @Test
    public void generateAdvertisement_withTargetCustomerIdInMarketplace_returnsAdvertisement() {

        GenerateAdvertisementRequest request = GenerateAdvertisementRequest.builder()
            .withCustomerId(PARENT_PROFILE_CUSTOMER_ID)
            .withMarketplaceId(US_MARKETPLACE_ID)
            .build();
        GenerateAdvertisementResponse result = new GenerateAdActivityDagger().handleRequest(request, null);

        assertNotNull(result.getAdvertisement(), "Expected a non null advertisement in the response.");
        assertNotNull(result.getAdvertisement().getId(), "Expected the advertisement to have a non-null " +
            "content ID.");
        assertFalse(StringUtils.isBlank(result.getAdvertisement().getContent()), "Expected a non-empty " +
            "advertisement content when generating an advertisement for a customer ID with a parent profile " +
            "in marketplace ID: " + request.getMarketplaceId());
    }

    @Test
    public void generateAdvertisement_withTargetCustomerIdNotInMarketplace_returnsEmptyContent() {
        GenerateAdvertisementRequest request = GenerateAdvertisementRequest.builder()
            .withCustomerId(EMPTY_PROFILE_CUSTOMER_ID)
            .withMarketplaceId(CA_MARKETPLACE_ID)
            .build();
        GenerateAdvertisementResponse result = new GenerateAdActivityDagger().handleRequest(request, null);

        assertNotNull(result.getAdvertisement(), "Expected a non null advertisement in the response.");
        assertNotNull(result.getAdvertisement().getId(), "Expected the advertisement to have a non-null " +
            "content ID.");
        assertTrue(StringUtils.isBlank(result.getAdvertisement().getContent()), "Expected an empty " +
            "advertisement content when generating an advertisement for a customer ID with an unknown profile " +
            "in marketplace ID: " + request.getMarketplaceId() + " the sulod is " + result.getAdvertisement().getContent());
    }

    @Test
    public void generateAdvertisement_withNonExistantMarketplace_returnsEmptyContent() {
        GenerateAdvertisementRequest request = GenerateAdvertisementRequest.builder()
            .withCustomerId(EMPTY_PROFILE_CUSTOMER_ID)
            .withMarketplaceId("TCT_TESTS_MARKETPLACE_ID")
            .build();
        GenerateAdvertisementResponse result = new GenerateAdActivityDagger().handleRequest(request, null);

        assertNotNull(result.getAdvertisement(), "Expected a non null advertisement in the response.");
        assertNotNull(result.getAdvertisement().getId(), "Expected the advertisement to have a non-null " +
            "content ID.");
        assertTrue(StringUtils.isBlank(result.getAdvertisement().getContent()), "Expected an empty " +
            "advertisement content when generating an advertisement for a customer ID with an unknown profile " +
            "in a non-existant marketplace ID: " + request.getMarketplaceId());
    }
}
