package com.crest.employee.leave.schedular;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.crest.employee.leave.handler.CsvDataExceptionHandler;
import com.crest.employee.leave.model.EmployeeLeaveBalance;
import com.crest.employee.leave.model.EmployeeLeaveRequest;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class LeaveUpdateService {

	private static final String EMPLOYEE_LEAVE_BALANCE_FILE = "EmployeeData.csv";
	private static final String EMPLOYEE_LEAVE_REQUEST_FILE = "Leaves.csv";

	@Value("${upload.path}")
	private String uploadPath;

	@Value("${employee.leave.data.csv.headers}")
	private String employeeLeaveDataCsvHeader;

	@Value("${employee.leave.data.csv.field.mapping}")
	private String employeeLeaveDataFieldMapping;

	@Value("${employee.leave.request.csv.headers}")
	private String employeeLeaveRequestCsvHeader;

	@Value("${employee.leave.request.csv.field.mapping}")
	private String employeeLeaveRequestFieldMapping;

	private List<EmployeeLeaveBalance> employeesLeaveBalance;

	private List<EmployeeLeaveRequest> employeesLeaveRequest;

	@PostConstruct
	public void init() {
		try {
			log.info("Leave Upload path is created : {}", uploadPath);
			Files.createDirectories(Paths.get(uploadPath));
			File uploadDirectory = new File(uploadPath);

			File employeeLeaveData = FileUtils.getFile(uploadDirectory, EMPLOYEE_LEAVE_BALANCE_FILE);

			if (employeeLeaveData.exists()) {
				populateEmployeeLeaveBalance(uploadDirectory);
			} else {
				throw new RuntimeException("Employee leave balance file not found");
			}
		} catch (IOException e) {
			throw new RuntimeException("Error in setting up application ", e);
		}
	}

	@Scheduled(fixedRate = 6000)
	public void leaveUpdateTask() {
		try {
			log.info("###############");
			log.info("Processing Leave requests - {}", new Date());
			File uploadDirectory = new File(uploadPath);
			File employeeLeaveRequestData = FileUtils.getFile(uploadDirectory, EMPLOYEE_LEAVE_REQUEST_FILE);

			if (!FileUtils.isEmptyDirectory(uploadDirectory) && employeeLeaveRequestData.exists()) {
				populateEmployeeLeaveRequest(uploadDirectory);
				processLeaveRequests();
				updateEmployeeLeaveData();
			} else {
				log.info("Leaves.csv files should be present in the leave directory");
			}
			log.info("###############");
		} catch (Exception e) {
			log.error("Error in reading leave related files : ", e);
		}
	}

	private void populateEmployeeLeaveBalance(File uploadDirectory) {
		try {
			File employeeLeaveData = FileUtils.getFile(uploadDirectory, EMPLOYEE_LEAVE_BALANCE_FILE);
			CsvDataExceptionHandler csvDataExceptionHandler = new CsvDataExceptionHandler();
			populateEmployeesLeavesBalance(employeeLeaveData, csvDataExceptionHandler);
			if (!csvDataExceptionHandler.getErrorRecords().isEmpty()) {
				log.info("Error records found in populating Employee Leave Balance");
				csvDataExceptionHandler.getErrorRecords().forEach(e -> log.info(e));
			}
		} catch (Exception e) {
			log.error("Error in reading Employee leave balance file : ", e);
		}
	}

	private void populateEmployeeLeaveRequest(File uploadDirectory) {
		try {
			File employeeLeaveRequestData = FileUtils.getFile(uploadDirectory, EMPLOYEE_LEAVE_REQUEST_FILE);

			CsvDataExceptionHandler csvDataExceptionHandler = new CsvDataExceptionHandler();
			populateEmployeesLeavesRequest(employeeLeaveRequestData, csvDataExceptionHandler);
			if (!csvDataExceptionHandler.getErrorRecords().isEmpty()) {
				log.info("Error records found in populating Employee Leave Request");
				csvDataExceptionHandler.getErrorRecords().forEach(e -> log.info(e));
			}
		} catch (Exception e) {
			log.error("Error in reading Employee leave request file : ", e);
		}
	}

	private void populateEmployeesLeavesBalance(File file, CsvDataExceptionHandler csvDataExceptionHandler) {

		List<String> csvHeaders = Arrays.asList(StringUtils.split(employeeLeaveDataCsvHeader, ","));
		List<String> fieldMappings = Arrays.asList(StringUtils.split(employeeLeaveDataFieldMapping, ","));

		Map<String, String> mapping = IntStream.range(0, csvHeaders.size()).boxed()
				.collect(Collectors.toMap(i -> csvHeaders.get(i), i -> fieldMappings.get(i)));

		HeaderColumnNameTranslateMappingStrategy<EmployeeLeaveBalance> strategy = new HeaderColumnNameTranslateMappingStrategy<>();
		strategy.setType(EmployeeLeaveBalance.class);
		strategy.setColumnMapping(mapping);

		CSVReader csvReader = null;

		try {
			csvReader = new CSVReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		CsvToBeanBuilder<EmployeeLeaveBalance> csvToBeanBuilder = new CsvToBeanBuilder<>(csvReader);
		CsvToBean<EmployeeLeaveBalance> csvToBean = csvToBeanBuilder.withMappingStrategy(strategy).withQuoteChar('"')
				.withExceptionHandler(csvDataExceptionHandler).build();

		employeesLeaveBalance = csvToBean.parse();
	}

	private void populateEmployeesLeavesRequest(File file, CsvDataExceptionHandler csvDataExceptionHandler) {

		List<String> csvHeaders = Arrays.asList(StringUtils.split(employeeLeaveRequestCsvHeader, ","));
		List<String> fieldMappings = Arrays.asList(StringUtils.split(employeeLeaveRequestFieldMapping, ","));

		Map<String, String> mapping = IntStream.range(0, csvHeaders.size()).boxed()
				.collect(Collectors.toMap(i -> csvHeaders.get(i), i -> fieldMappings.get(i)));

		HeaderColumnNameTranslateMappingStrategy<EmployeeLeaveRequest> strategy = new HeaderColumnNameTranslateMappingStrategy<>();
		strategy.setType(EmployeeLeaveRequest.class);
		strategy.setColumnMapping(mapping);

		CSVReader csvReader = null;

		try {
			csvReader = new CSVReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		CsvToBeanBuilder<EmployeeLeaveRequest> csvToBeanBuilder = new CsvToBeanBuilder<>(csvReader);
		CsvToBean<EmployeeLeaveRequest> csvToBean = csvToBeanBuilder.withMappingStrategy(strategy).withQuoteChar('"')
				.withExceptionHandler(csvDataExceptionHandler).build();

		employeesLeaveRequest = csvToBean.parse();
	}

	private void processLeaveRequests() {

		if (!employeesLeaveBalance.isEmpty() && !employeesLeaveRequest.isEmpty()) {
			employeesLeaveRequest.forEach(req -> validateLeaveRequest(req.getEmployeeId()));
		} else {
			log.info("Either Employee Balance OR Employee Leave Requests are empty");
		}
	}

	private void validateLeaveRequest(int employeeId) {
		EmployeeLeaveBalance employeeLeaveBalance = employeesLeaveBalance.stream()
				.filter(elb -> elb.getEmployeeId() == employeeId).findFirst().orElse(null);
		EmployeeLeaveRequest employeeLeaveRequest = employeesLeaveRequest.stream()
				.filter(elb -> elb.getEmployeeId() == employeeId).findFirst().orElse(null);

		log.info("***********");
		if (Objects.nonNull(employeeLeaveBalance)) {
			log.info("Employee ID: {} Available Leave Balance: {} Applied Leave: {}", employeeId,
					employeeLeaveBalance.getAvailableLeaves(), employeeLeaveRequest.getAppliedLeaves());
			
			if (employeeLeaveRequest.getAppliedLeaves() < 0) {
				log.info("Applied leave is negative value : {}", employeeLeaveRequest.getAppliedLeaves());
				return;
			}
			
			if (Objects.nonNull(employeeLeaveBalance) && Objects.nonNull(employeeLeaveRequest)
					&& employeeLeaveBalance.getAvailableLeaves() >= employeeLeaveRequest.getAppliedLeaves()) {
				log.info("Employee ID : {} is eligible to take leave", employeeId);
				int availableLeaves = employeeLeaveBalance.getAvailableLeaves();
				int leavesTaken = employeeLeaveBalance.getLeavesTaken();
				int appliedLeaves = employeeLeaveRequest.getAppliedLeaves();
				employeeLeaveBalance.setAvailableLeaves(availableLeaves - appliedLeaves);
				employeeLeaveBalance.setLeavesTaken(leavesTaken + appliedLeaves);
			} else {
				log.info("Employee ID : {} is NOT eligible to take leave", employeeId);
			}
		} else {
			log.info("Employee ID : {} is NOT found in Employee Leaves data", employeeId);
		}
		log.info("***********");
	}

	private void updateEmployeeLeaveData() {
		try {
			File uploadDirectory = new File(uploadPath);
			File employeeLeaveData = FileUtils.getFile(uploadDirectory, EMPLOYEE_LEAVE_BALANCE_FILE);

			FileUtils.writeStringToFile(employeeLeaveData, employeeLeaveDataCsvHeader + "\n", Charset.defaultCharset(),
					false);
			FileUtils.writeLines(employeeLeaveData, employeesLeaveBalance, true);

			File employeeLeaveRequestData = FileUtils.getFile(uploadDirectory, EMPLOYEE_LEAVE_REQUEST_FILE);
			FileUtils.write(employeeLeaveRequestData, employeeLeaveRequestCsvHeader, Charset.defaultCharset(), false);
		} catch (Exception e) {
			log.info("Error in updating employee leave data ", e);
		}
	}
}
