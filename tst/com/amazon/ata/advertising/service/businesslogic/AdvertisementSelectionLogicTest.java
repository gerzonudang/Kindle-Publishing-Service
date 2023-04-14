package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.AdvertisementContent;
import com.amazon.ata.advertising.service.model.EmptyGeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.GeneratedAdvertisement;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class AdvertisementSelectionLogicTest {

    private static final String CUSTOMER_ID = "A123B456";
    private static final String MARKETPLACE_ID = "1";

    private static final String CONTENT_ID1 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT1 = AdvertisementContent.builder().withContentId(CONTENT_ID1).build();
    private static final String CONTENT_ID2 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT2 = AdvertisementContent.builder().withContentId(CONTENT_ID2).build();
    private static final String CONTENT_ID3 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT3 = AdvertisementContent.builder().withContentId(CONTENT_ID3).build();
    private static final String CONTENT_ID4 = UUID.randomUUID().toString();
    private static final AdvertisementContent CONTENT4 = AdvertisementContent.builder().withContentId(CONTENT_ID4).build();

    @Mock
    private ReadableDao<String, List<AdvertisementContent>> contentDao;

    @Mock
    private ReadableDao<String, List<TargetingGroup>> targetingGroupDao;

    @Mock
    private Random random;

    private AdvertisementSelectionLogic adSelectionService;


    @BeforeEach
    public void setup() {
        initMocks(this);
        adSelectionService = new AdvertisementSelectionLogic(contentDao, targetingGroupDao);
        adSelectionService.setRandom(random);
    }

    @Test
    public void selectAdvertisement_nullMarketplaceId_EmptyAdReturned() {
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, null);
        assertTrue(ad instanceof EmptyGeneratedAdvertisement);
    }

    @Test
    public void selectAdvertisement_emptyMarketplaceId_EmptyAdReturned() {
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, "");
        assertTrue(ad instanceof EmptyGeneratedAdvertisement);
    }

    @Test
    public void selectAdvertisement_noContentForMarketplace_emptyAdReturned() throws InterruptedException {
        when(contentDao.get(MARKETPLACE_ID)).thenReturn(Collections.emptyList());

        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, MARKETPLACE_ID);

        assertTrue(ad instanceof EmptyGeneratedAdvertisement);
    }


    @Test
    public void selectAdvertisement_oneAd_returnsAd() {
        List<AdvertisementContent> contents = Arrays.asList(CONTENT1);
        when(contentDao.get(MARKETPLACE_ID)).thenReturn(contents);
        when(random.nextInt(contents.size())).thenReturn(0);
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, MARKETPLACE_ID);

        assertEquals(CONTENT_ID1, ad.getContent().getContentId());
    }

    @Test
    public void selectAdvertisement_multipleAds_returnsOneRandom() {
        List<AdvertisementContent> contents = Arrays.asList(CONTENT1, CONTENT2, CONTENT3);
        when(contentDao.get(MARKETPLACE_ID)).thenReturn(contents);
        when(random.nextInt(contents.size())).thenReturn(1);
        GeneratedAdvertisement ad = adSelectionService.selectAdvertisement(CUSTOMER_ID, MARKETPLACE_ID);

        assertEquals(CONTENT_ID2, ad.getContent().getContentId());
    }

}
